import uvicorn
import asyncio
import logging
from datetime import datetime, timedelta, timezone
import os
from typing import Dict, List, Any

from fastapi import FastAPI, HTTPException, Depends, Query, APIRouter
from pydantic import BaseModel, Field
import json

# Importa componentes locais
from src.config_manager import ConfigManager, MachineConfigModel
from src.plc_connector import PLCConnector
from src.database import Database
from src.calculations import EfficiencyCalculator
from src.api.endpoints import router as api_router # Importa o router da API
from src.api.models import MachineDataRaw, EfficiencyMetrics # Importa modelos Pydantic para API

# --- Configuração Inicial ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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

# Inclui as rotas da API definidas em endpoints.py
app.include_router(api_router)

# --- Funções Auxiliares para o Ciclo Principal ---

async def fetch_and_store_data():
    """Coleta dados de todas as máquinas configuradas e armazena no banco de dados."""
    logging.info("Iniciando coleta de dados do PLC...")
    all_machine_names = config_manager.get_all_machine_names()
    
    if not all_machine_names:
        logging.warning("Nenhuma máquina configurada encontrada. Aguardando configurações...")
        return

    data_to_insert = []
    for machine_name in all_machine_names:
        machine_config = config_manager.get_machine_config(machine_name)
        if not machine_config:
            logging.warning(f"Configuração não encontrada para {machine_name}. Pulando coleta.")
            continue

        # Define as tags que queremos ler de cada máquina
        # Usaremos `status` e `total_strokes` que são essenciais para nossos cálculos
        # E também `current_speed_spm` e `max_sp` para performance
        tags_to_read_keys = ["status", "total_strokes", "current_speed_spm", "max_sp"]
        
        # Adiciona outras tags mapeadas se necessário para log ou futuras análises
        # tags_to_read_keys.extend([k for k in machine_config.tag_mapping if k not in tags_to_read_keys])

        # Lê as tags
        # Usamos `read_multiple_tags` para otimizar a comunicação com o PLC
        read_values = plc_connector.read_multiple_tags(machine_name, tags_to_read_keys)

        if read_values and any(v is not None for v in read_values.values()): # Se alguma leitura foi bem sucedida
            data_record: Dict[str, Any] = {"machine_name": machine_name}
            
            # Mapeia os valores lidos para as chaves genéricas do banco de dados
            data_record["status"] = read_values.get("status")
            data_record["total_strokes"] = read_values.get("total_strokes")
            data_record["current_speed_spm"] = read_values.get("current_speed_spm")
            data_record["max_sp"] = read_values.get("max_sp")
            data_record["min_sp"] = read_values.get("min_sp") # Se você quiser logar isso também

            # Adiciona dados que podem vir de outras tags mapeadas, se necessário
            # Ex: data_record["line_control_discharge_high"] = read_values.get("Line_Control_Discharge_High")

            data_to_insert.append(data_record)
        else:
            logging.warning(f"Nenhum dado válido lido para a máquina {machine_name} nesta coleta.")

    if data_to_insert:
        db.insert_data_batch(data_to_insert)

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
        # Busca todos os registros brutos recentes que ainda não tiveram seus intervalos calculados
        # Uma forma de fazer isso é buscar registros com interval_run_time_seconds = 0 e machine_name
        # Ou buscar registros de um período recente (ex: últimos 5 minutos)
        
        # Vamos buscar todos os registros de todas as máquinas do último PROCESSING_INTERVAL_SECONDS
        # Isso pode ser caro se houver muitos dados. Uma otimização é buscar apenas os não processados.
        
        # Opção 1: Buscar dados recentes e recalcular
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
@app.on_event("startup")
async def startup_event():
    """Executado quando a aplicação FastAPI inicia."""
    logging.info("Aplicação FastAPI iniciado.")
    # Garante que o banco de dados esteja pronto
    try:
        db._create_tables() # Chama explicitamente para garantir que a tabela exista
        logging.info("Banco de dados pronto.")
    except Exception as e:
        logging.critical(f"Falha crítica ao inicializar o banco de dados: {e}")
        # Você pode querer parar a aplicação aqui se o DB for essencial
        return

    # Carrega todas as configurações de máquinas
    config_manager.load_all_configs()
    logging.info(f"{len(config_manager.get_all_machine_names())} máquinas carregadas.")

    # Abre as conexões iniciais com os PLCs
    # O `plc_connector.connect()` será chamado quando a primeira leitura for necessária.
    # Aqui podemos apenas garantir que o manager esteja pronto.

    # Inicia o loop principal em uma task separada para não bloquear o loop de eventos do FastAPI
    asyncio.create_task(main_loop())
    logging.info("Loop principal de monitoramento iniciado em background.")

@app.on_event("shutdown")
async def shutdown_event():
    """Executado quando a aplicação FastAPI é encerrada."""
    logging.info("Aplicação FastAPI sendo encerrada.")
    # Fecha todas as conexões com os PLCs
    plc_connector.close_connections()
    logging.info("Conexões com PLC fechadas.")

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

    logging.info("Iniciando servidor Uvicorn (para rodar com 'python src/main.py')...")
    logging.info("Acesse a API em: http://127.0.0.1:8000")
    logging.info("Documentação interativa (Swagger UI): http://127.0.0.1:8000/docs")
    uvicorn.run(app, host="127.0.0.1", port=8000)
