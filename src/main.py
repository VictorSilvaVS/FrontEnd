import uvicorn
import asyncio
import logging
import os
import sys
import json

# --- Correção de Path para evitar "ModuleNotFoundError: No module named 'src'" ---
# Isso permite rodar tanto como 'python src/main.py' quanto 'python -m src.main'
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional
from logging.handlers import RotatingFileHandler

# Importa bibliotecas para agendamento e timezone (Precisa de pip install apscheduler pytz)
try:
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    from apscheduler.triggers.cron import CronTrigger
    import pytz
except ImportError:
    logging.error("Bibliotecas 'apscheduler' ou 'pytz' não encontradas. Instale com: pip install apscheduler pytz")

from fastapi import FastAPI, HTTPException, Depends, Query, APIRouter, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

# Importa componentes locais
from src.config_manager import ConfigManager, MachineConfigModel
from src.plc_connector import PLCConnector
from src.database import Database
from src.calculations import EfficiencyCalculator
from src.api.models import MachineDataRaw, EfficiencyMetrics
from src.teams_notifier import TeamsNotifier

# --- Configuração de Pastas ---
LOGS_DIR = "logs"
os.makedirs(LOGS_DIR, exist_ok=True)

# --- Timezone Brasil ---
BR_TZ = pytz.timezone('America/Sao_Paulo')

# --- Configuração de Logging ---
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - [%(name)s] - %(message)s')
log_file = os.path.join(LOGS_DIR, "machine_monitor.log")
file_handler = RotatingFileHandler(log_file, maxBytes=5*1024*1024, backupCount=5)
file_handler.setFormatter(log_formatter)
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)

logging.basicConfig(level=logging.INFO, handlers=[file_handler, console_handler])
logger = logging.getLogger("Main")

# --- Instanciação dos Componentes ---
config_manager = ConfigManager(configs_dir="configs/machines")
db = Database(db_path="database/production_data.db", config_manager=config_manager)
plc_connector = PLCConnector(config_manager=config_manager)
calculator = EfficiencyCalculator(db_path="database/production_data.db", config_manager=config_manager)
teams_notifier = TeamsNotifier()

# --- Gerenciador de WebSockets (Tempo Real) ---
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except:
                pass

manager = ConnectionManager()

# Caminhos de configuração
CONFIGS_MACHINES_DIR = "configs/machines"
DB_PATH = "database/production_data.db"
PLC_CONFIG_FILE = "configs/plc_config.json" # Se você tiver um arquivo centralizado para PLCs
STANDBY_CODES_FILE = "configs/standby_codes.json" # Se você tiver um arquivo centralizado para códigos

# Intervalos de tempo (em segundos)
COLLECTION_INTERVAL_SECONDS = 10 # Frequência de coleta de dados do PLC
PROCESSING_INTERVAL_SECONDS = 60 # Frequência para calcular e atualizar tempos de intervalo no DB
REPORTING_INTERVAL_SECONDS = 300 # Frequência para calcular métricas de OEE (ex: a cada 5 minutos)

# --- Instanciação dos Componentes Principais ---
# É importante instanciar ConfigManager primeiro, pois outros componentes dependem dele.
config_manager = ConfigManager(configs_dir=CONFIGS_MACHINES_DIR)
db = Database(db_path=DB_PATH)
plc_connector = PLCConnector(config_manager=config_manager)
calculator = EfficiencyCalculator(db_path=DB_PATH, config_manager=config_manager)

# --- Criação da Aplicação FastAPI ---
app = FastAPI(
    title="Machine Monitoring API",
    description="API para monitoramento de produção de máquinas industriais.",
    version="1.0.0",
)

# --- Configuração de CORS (Essencial para integração com FrontEnd) ---
from fastapi.middleware.cors import CORSMiddleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Em produção, substitua pelo IP/DNS do seu FrontEnd
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Inclui as rotas da API definidas em endpoints.py
from src.api import endpoints
endpoints.set_dependencies(db, calculator, plc_connector, config_manager)
app.include_router(endpoints.router)

# --- Funções Auxiliares para o Ciclo Principal ---

async def fetch_and_store_data():
    """Coleta dados, armazena e faz o broadcast via WebSocket."""
    # logger.info("Iniciando coleta de dados do PLC...")
    all_machine_names = config_manager.get_all_machine_names()
    
    if not all_machine_names: return

    data_to_insert = []
    real_time_data = {}

    for machine_name in all_machine_names:
        tags_to_read_keys = ["status", "total_strokes", "current_speed_spm", "max_sp"]
        read_values = plc_connector.read_multiple_tags(machine_name, tags_to_read_keys)

        if read_values and any(v is not None for v in read_values.values()):
            data_record = {"machine_name": machine_name}
            data_record.update({k: read_values.get(k) for k in tags_to_read_keys})
            data_to_insert.append(data_record)
            real_time_data[machine_name] = data_record

    if data_to_insert:
        db.insert_data_batch(data_to_insert)
        # Envia para todos os clientes conectados via WebSocket
        await manager.broadcast(json.dumps({
            "type": "real_time_update",
            "timestamp": datetime.now(BR_TZ).isoformat(),
            "data": real_time_data
        }))

async def run_hourly_rollup():
    """Calcula o resumo da hora anterior e salva no banco (Rollup)."""
    now = datetime.now(BR_TZ)
    # Busca dados da hora cheia anterior
    end_time = now.replace(minute=0, second=0, microsecond=0)
    start_time = end_time - timedelta(hours=1)
    
    logger.info(f"Iniciando Rollup Horário: {start_time.hour}h até {end_time.hour}h")
    
    for machine_name in config_manager.get_all_machine_names():
        try:
            data_points = db.get_data_for_period(start_time, end_time, machine_name)
            if data_points:
                metrics = calculator.calculate_metrics_for_period(machine_name, data_points)
                rollup = {
                    "machine_name": machine_name,
                    "hour_timestamp": start_time.isoformat(),
                    "total_production": metrics.get("total_strokes", 0),
                    "run_time_seconds": metrics.get("operating_time_seconds", 0),
                    "standby_time_seconds": metrics.get("standby_downtime_seconds", 0),
                    "availability": metrics.get("availability_ratio", 0.0),
                    "performance": metrics.get("performance_ratio", 0.0),
                    "oee": metrics.get("oee", 0.0)
                }
                db.insert_hourly_rollup(rollup)
        except Exception as e:
            logger.error(f"Erro no rollup horário para {machine_name}: {e}")

async def send_shift_report():
    """Envia 2 cards para o Teams no fechamento de turno (6h e 18h)."""
    now = datetime.now(BR_TZ)
    # Turno de 12 horas
    start_time = now - timedelta(hours=12)
    end_time = now
    
    logger.info(f"Fechamento de Turno detectado: {now.strftime('%H:%M')}. Enviando relatórios...")
    
    # Agrupa por linha (Linha 22 e Linha 23)
    lines = {"22": [], "23": []}
    
    for machine_name in config_manager.get_all_machine_names():
        config = config_manager.get_machine_config(machine_name)
        line = config.line_number if config and config.line_number in ["22", "23"] else None
        
        if line:
            data = db.get_data_for_period(start_time, end_time, machine_name)
            if data:
                metrics = calculator.calculate_metrics_for_period(machine_name, data)
                lines[line].append({
                    "name": machine_name,
                    "oee": metrics.get("oee", 0.0),
                    "prod": metrics.get("total_strokes", 0),
                    "standby": metrics.get("standby_downtime_seconds", 0)
                })

    for line_id, machine_results in lines.items():
        if not machine_results: continue
        
        # Cria um card consolidado para a linha
        # (Neste exemplo, mandamos 1 card por máquina, conforme pedido '2 cards' se houver 2 máquinas ou 
        # podemos consolidar. Vamos mandar 1 card por linha se for o esperado ou 1 por linha contendo as máquinas).
        # O usuário pediu '2 card, 1 para cada linha'. Vamos criar um resumo por linha.
        
        avg_oee = sum(m['oee'] for m in machine_results) / len(machine_results)
        total_prod = sum(m['prod'] for m in machine_results)
        total_standby = sum(m['standby'] for m in machine_results)
        
        footer = f"Relatório de Turno ({start_time.strftime('%H:%M')} - {end_time.strftime('%H:%M')})"
        
        card = teams_notifier.build_card_payload(
            machine_name=f"LINHA {line_id}",
            efficiency=avg_oee,
            production=total_prod,
            standby_seconds=total_standby,
            footer=footer
        )
        teams_notifier.send_message(card)

async def update_interval_times_in_db():
    """
    Calcula e atualiza os tempos de intervalo (run_time, standby_time) no banco de dados.
    Este processo é crucial para que os cálculos de eficiência usem dados precisos.
    Deve rodar periodicamente após a inserção dos dados brutos.
    """
    logging.info("Atualizando tempos de intervalo no banco de dados...")
    all_machine_names = config_manager.get_all_machine_names()
    if not all_machine_names:
        logging.warning("Nenhuma máquina configurada para atualizar tempos de intervalo.")
        return

    # Define um período razoável para buscar dados para processamento
    # Buscar os registros que foram inseridos desde a última execução desta função
    # Uma abordagem mais simples é buscar todos os registros recentes e recalcular para eles.
    # Para otimizar, poderíamos armazenar o timestamp da última atualização e buscar a partir dele.
    
    # Método simples: buscar os últimos N registros por máquina e recalcular.
    # Para um sistema robusto, seria melhor ter uma tabela de "last_processed_timestamp" por máquina.
    
    try:

        end_time_processing = datetime.now(timezone.utc)
        start_time_processing = end_time_processing - timedelta(seconds=PROCESSING_INTERVAL_SECONDS * 2) # Busca um pouco mais para garantir
        
        all_recent_raw_data = db.get_data_for_period(start_time_processing, end_time_processing)
        
        if all_recent_raw_data:
            # Precisamos agrupar por máquina e garantir que os dados estejam ordenados para usar `update_interval_times`
            # `update_interval_times` já faz o agrupamento e ordenação interna.
            db.update_interval_times(all_recent_raw_data)
        else:
            logging.info("Nenhum dado bruto recente para processar tempos de intervalo.")
            
    except Exception as e:
        logging.error(f"Erro ao atualizar tempos de intervalo no DB: {e}")

async def calculate_and_report_oee():
    """
    Calcula as métricas de OEE para cada máquina em um período definido
    e pode opcionalmente enviar para um sistema de relatórios/dashboard.
    """
    logging.info("Calculando métricas de OEE...")
    all_machine_names = config_manager.get_all_machine_names()
    
    if not all_machine_names:
        logging.warning("Nenhuma máquina configurada para calcular OEE.")
        return

    # Define o período para o cálculo do OEE (ex: última hora, último turno, etc.)
    # Para este exemplo, vamos calcular para a última REPORTING_INTERVAL_SECONDS.
    # Em um cenário real, você pode querer calcular para períodos fixos (ex: hora a hora).
    
    end_time_oee = datetime.now(timezone.utc)
    start_time_oee = end_time_oee - timedelta(seconds=REPORTING_INTERVAL_SECONDS)

    # Você pode querer calcular OEE para períodos mais longos também (ex: últimas 24h)
    # start_time_24h = end_time_oee - timedelta(hours=24)
    # data_24h = db.get_data_for_period(start_time_24h, end_time_oee) # Não filtramos por máquina aqui, pode ser pesado.

    for machine_name in all_machine_names:
        try:
            # Busca os dados do período específico para a máquina
            data_points_for_machine = db.get_data_for_period(start_time_oee, end_time_oee, machine_name)
            
            if not data_points_for_machine:
                logging.warning(f"Nenhum dado encontrado para {machine_name} no período de OEE ({start_time_oee} a {end_time_oee}).")
                continue

            # Calcula as métricas usando os dados que JÁ TÊM os tempos de intervalo calculados
            metrics = calculator.calculate_metrics_for_period(machine_name, data_points_for_machine)
            
            # Aqui você pode:
            # 1. Salvar essas métricas em outra tabela no DB para histórico de OEE.
            # 2. Enviar para um sistema de dashboard (ex: InfluxDB, Grafana, sistema de nuvem).
            # 3. Apenas logar para debug (como feito abaixo).
            
            logging.info(f"OEE Metrics for {machine_name} ({start_time_oee.isoformat()} to {end_time_oee.isoformat()}):")
            # Loga apenas os valores principais para não poluir muito
            logging.info(f"  - OEE: {metrics.get('oee', 0.0):.2%}")
            logging.info(f"  - Availability: {metrics.get('availability_ratio', 0.0):.2%}")
            logging.info(f"  - Performance: {metrics.get('performance_ratio', 0.0):.2%}")
            logging.info(f"  - Total Strokes: {metrics.get('total_strokes', 0)}")
            logging.info(f"  - Available Time (s): {metrics.get('available_time_seconds', 0)}")
            logging.info(f"  - Operating Time (s): {metrics.get('operating_time_seconds', 0)}")
            logging.info(f"  - Downtime SC (s): {metrics.get('standby_downtime_seconds', 0)}")

        except Exception as e:
            logging.error(f"Erro ao calcular OEE para {machine_name}: {e}")

# --- Loop Principal de Tarefas Assíncronas ---

async def main_loop():
    """Orquestra as tarefas periódicas: coleta, processamento de intervalo e cálculo de OEE."""
    logging.info("Iniciando o loop principal de monitoramento...")

    # Define os timers para cada tarefa
    collection_timer = asyncio.create_task(periodic_task(COLLECTION_INTERVAL_SECONDS, fetch_and_store_data))
    processing_timer = asyncio.create_task(periodic_task(PROCESSING_INTERVAL_SECONDS, update_interval_times_in_db))
    reporting_timer = asyncio.create_task(periodic_task(REPORTING_INTERVAL_SECONDS, calculate_and_report_oee))

    # Mantém o loop rodando indefinidamente
    await asyncio.gather(collection_timer, processing_timer, reporting_timer)

async def periodic_task(interval: int, coro):
    """Executa uma corrotina (coro) periodicamente com um intervalo fixo."""
    while True:
        try:
            await coro()
        except Exception as e:
            logging.error(f"Erro na tarefa periódica {coro.__name__}: {e}")
        await asyncio.sleep(interval)

# --- Eventos de Lifecycle da Aplicação FastAPI ---
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text() # Mantém conexão viva
    except WebSocketDisconnect:
        manager.disconnect(websocket)

@app.on_event("startup")
async def startup_event():
    """Executado quando a aplicação FastAPI inicia."""
    logger.info("Aplicação FastAPI iniciada.")
    db._create_tables()
    config_manager.load_all_configs()
    
    # --- Configura o Agendador (APScheduler) ---
    scheduler = AsyncIOScheduler(timezone=BR_TZ)
    
    # Coleta a cada 10s (já estava assim)
    scheduler.add_job(fetch_and_store_data, 'interval', seconds=10)
    
    # Processamento de intervalos para o DB a cada 1m
    scheduler.add_job(update_interval_times_in_db, 'interval', minutes=1)
    
    # Rollup Horário (todo início de hora)
    scheduler.add_job(run_hourly_rollup, CronTrigger(minute=0, timezone=BR_TZ))
    
    # Relatório de Turno (6h e 18h)
    scheduler.add_job(send_shift_report, CronTrigger(hour='6,18', minute=0, timezone=BR_TZ))
    
    scheduler.start()
    logger.info("Agendador (Scheduler) iniciado com sucesso.")

@app.on_event("shutdown")
async def shutdown_event():
    """Executado quando a aplicação FastAPI é encerrada."""
    logger.info("Aplicação FastAPI sendo encerrada.")
    plc_connector.close_connections()

# --- Endpoint Raiz (Opcional) ---
@app.get("/", tags=["Root"])
async def read_root():
    return {"message": "Bem-vindo à API de Monitoramento de Produção!"}

# --- Execução do Servidor Uvicorn (para rodar diretamente) ---
# Este bloco é para rodar o script como um executável Python simples.
# Em produção, você usará `uvicorn src.main:app --host ...` ou Gunicorn.
if __name__ == "__main__":
    # Para rodar este script diretamente: python src/main.py
    # Você precisará criar as pastas `configs/machines/` e `database/` manualmente
    # e adicionar pelo menos um arquivo JSON em `configs/machines/`.
    
    # Exemplo de criação manual de arquivos de config (para testes)
    if not os.path.exists("configs/machines"):
        os.makedirs("configs/machines")
        logging.info("Criado: configs/machines/")
    if not os.path.exists("database"):
        os.makedirs("database")
        logging.info("Criado: database/")

    # Cria um arquivo de configuração de exemplo se não existir
    example_machine_config_path = os.path.join("configs/machines", "example_machine.json")
    if not os.path.exists(example_machine_config_path):
        example_config_data = {
            "name": "Example_Machine_01",
            "ip_address": "192.168.1.100", # Mude para um IP válido ou de teste
            "processor_slot": 1,
            "tags_to_read": ["oPV_Shift_Stroke_Count", "Machine_Speed_SPM", "IGN.Status"],
            "tag_mapping": {
                "total_strokes": "oPV_Shift_Stroke_Count",
                "current_speed_spm": "Machine_Speed_SPM",
                "status": "IGN.Status",
                "max_sp": "CN1_BM102.Command.High_Speed_SP" # Exemplo de tag para max_sp
            },
            "standby_codes": [5, 6, 9, 10, 84],
            "line_number": "1"
        }
        with open(example_machine_config_path, 'w') as f:
            json.dump(example_config_data, f, indent=4)
        logging.info(f"Criado arquivo de configuração de exemplo: {example_machine_config_path}")

    # Cria o arquivo de standby_codes.json se não existir
    if not os.path.exists(STANDBY_CODES_FILE):
        standby_data = {"standby_codes": [5, 6, 9, 10, 84]}
        with open(STANDBY_CODES_FILE, 'w') as f:
            json.dump(standby_data, f, indent=4)
        logging.info(f"Criado arquivo de configuração de standby codes: {STANDBY_CODES_FILE}")

    logging.info("Iniciando servidor Uvicorn (Acesso externo habilitado)...")
    logging.info("Acesse a API local em: http://127.0.0.1:8000")
    logging.info("Documentação interativa (Swagger UI): http://127.0.0.1:8000/docs")
    # Bind em 0.0.0.0 permite que outros computadores na rede acessem este serviço
    uvicorn.run(app, host="0.0.0.0", port=8000)
