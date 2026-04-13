import uvicorn
import asyncio
import logging
import os
import sys
import json
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional
from logging.handlers import RotatingFileHandler
try:
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    from apscheduler.triggers.cron import CronTrigger
    import pytz
except ImportError:
    logging.error("Bibliotecas 'apscheduler' ou 'pytz' não encontradas. Instale com: pip install apscheduler pytz")
from fastapi import FastAPI, HTTPException, Depends, Query, APIRouter, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from dotenv import load_dotenv
from src.config_manager import ConfigManager, MachineConfigModel
from src.plc_connector import PLCConnector
from src.database import Database
from src.calculations import EfficiencyCalculator
from src.api.models import MachineDataRaw, EfficiencyMetrics
from src.teams_notifier import TeamsNotifier
load_dotenv()
LOG_LEVEL = os.environ["LOG_LEVEL"].upper()
LOGS_DIR = os.environ["LOGS_DIR"]
BR_TZ = pytz.timezone(os.environ["TIMEZONE"])

CONFIGS_MACHINES_DIR = os.environ["CONFIGS_MACHINES_DIR"]
DB_PATH = os.environ["DB_PATH"]
PLC_CONFIG_FILE = os.environ["PLC_CONFIG_FILE"]
STANDBY_CODES_FILE = os.environ["STANDBY_CODES_FILE"]

COLLECTION_INTERVAL_SECONDS = int(os.environ["COLLECTION_INTERVAL_SECONDS"])
PROCESSING_INTERVAL_SECONDS = int(os.environ["PROCESSING_INTERVAL_SECONDS"])
REPORTING_INTERVAL_SECONDS = int(os.environ["REPORTING_INTERVAL_SECONDS"])

SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])

TEAMS_WEBHOOK_URL = os.environ.get("TEAMS_WEBHOOK_URL")
HTTP_PROXY = os.environ.get("HTTP_PROXY")
HTTPS_PROXY = os.environ.get("HTTPS_PROXY")

os.makedirs(LOGS_DIR, exist_ok=True)
os.makedirs(CONFIGS_MACHINES_DIR, exist_ok=True)
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - [%(name)s] - %(message)s')
log_file = os.path.join(LOGS_DIR, "machine_monitor.log")

file_handler = RotatingFileHandler(log_file, maxBytes=5*1024*1024, backupCount=5)
file_handler.setFormatter(log_formatter)

console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)

numeric_level = getattr(logging, LOG_LEVEL, logging.INFO)
logging.basicConfig(level=numeric_level, handlers=[file_handler, console_handler])
logger = logging.getLogger("SystemCore")

config_manager = ConfigManager(configs_dir=CONFIGS_MACHINES_DIR)
db = Database(db_path=DB_PATH, config_manager=config_manager)
plc_connector = PLCConnector(config_manager=config_manager)
calculator = EfficiencyCalculator(db_path=DB_PATH, config_manager=config_manager)
teams_proxies = {"http": HTTP_PROXY, "https": HTTPS_PROXY} if HTTP_PROXY or HTTPS_PROXY else None
teams_notifier = TeamsNotifier(webhook_url=TEAMS_WEBHOOK_URL, proxies=teams_proxies)

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
            except Exception as e:
                logger.debug(f"Erro ao transmitir websocket: {e}")

manager = ConnectionManager()

app = FastAPI(
    title="Machine Monitoring API",
    description="API para monitoramento de produção de máquinas industriais.",
    version="1.0.0",
)

from fastapi.middleware.cors import CORSMiddleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

from src.api import endpoints
endpoints.set_dependencies(db, calculator, plc_connector, config_manager)
app.include_router(endpoints.router)

async def fetch_and_store_data():
    """Coleta dados, armazena e faz o broadcast via WebSocket."""

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

        await manager.broadcast(json.dumps({
            "type": "real_time_update",
            "timestamp": datetime.now(BR_TZ).isoformat(),
            "data": real_time_data
        }))

async def run_hourly_rollup():
    """Calcula o resumo da hora anterior e salva no banco (Rollup)."""
    now = datetime.now(BR_TZ)

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

    start_time = now - timedelta(hours=12)
    end_time = now

    logger.info(f"Fechamento de Turno detectado: {now.strftime('%H:%M')}. Enviando relatórios...")

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

    try:

        end_time_processing = datetime.now(timezone.utc)
        start_time_processing = end_time_processing - timedelta(seconds=PROCESSING_INTERVAL_SECONDS * 2)

        all_recent_raw_data = db.get_data_for_period(start_time_processing, end_time_processing)

        if all_recent_raw_data:

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

    end_time_oee = datetime.now(timezone.utc)
    start_time_oee = end_time_oee - timedelta(seconds=REPORTING_INTERVAL_SECONDS)

    for machine_name in all_machine_names:
        try:

            data_points_for_machine = db.get_data_for_period(start_time_oee, end_time_oee, machine_name)

            if not data_points_for_machine:
                logging.warning(f"Nenhum dado encontrado para {machine_name} no período de OEE ({start_time_oee} a {end_time_oee}).")
                continue

            metrics = calculator.calculate_metrics_for_period(machine_name, data_points_for_machine)

            logging.info(f"OEE Metrics for {machine_name} ({start_time_oee.isoformat()} to {end_time_oee.isoformat()}):")

            logging.info(f"  - OEE: {metrics.get('oee', 0.0):.2%}")
            logging.info(f"  - Availability: {metrics.get('availability_ratio', 0.0):.2%}")
            logging.info(f"  - Performance: {metrics.get('performance_ratio', 0.0):.2%}")
            logging.info(f"  - Total Strokes: {metrics.get('total_strokes', 0)}")
            logging.info(f"  - Available Time (s): {metrics.get('available_time_seconds', 0)}")
            logging.info(f"  - Operating Time (s): {metrics.get('operating_time_seconds', 0)}")
            logging.info(f"  - Downtime SC (s): {metrics.get('standby_downtime_seconds', 0)}")

        except Exception as e:
            logging.error(f"Erro ao calcular OEE para {machine_name}: {e}")

async def main_loop():
    """Orquestra as tarefas periódicas: coleta, processamento de intervalo e cálculo de OEE."""
    logging.info("Iniciando o loop principal de monitoramento...")

    collection_timer = asyncio.create_task(periodic_task(COLLECTION_INTERVAL_SECONDS, fetch_and_store_data))
    processing_timer = asyncio.create_task(periodic_task(PROCESSING_INTERVAL_SECONDS, update_interval_times_in_db))
    reporting_timer = asyncio.create_task(periodic_task(REPORTING_INTERVAL_SECONDS, calculate_and_report_oee))

    await asyncio.gather(collection_timer, processing_timer, reporting_timer)

async def periodic_task(interval: int, coro):
    """Executa uma corrotina (coro) periodicamente com um intervalo fixo."""
    while True:
        try:
            await coro()
        except Exception as e:
            logging.error(f"Erro na tarefa periódica {coro.__name__}: {e}")
        await asyncio.sleep(interval)

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)

@app.on_event("startup")
async def startup_event():
    """Executado quando a aplicação FastAPI inicia."""
    logger.info("Aplicação FastAPI iniciada.")
    db._create_tables()
    config_manager.load_all_configs()

    scheduler = AsyncIOScheduler(timezone=BR_TZ)

    scheduler.add_job(fetch_and_store_data, 'interval', seconds=10)

    scheduler.add_job(update_interval_times_in_db, 'interval', minutes=1)

    scheduler.add_job(run_hourly_rollup, CronTrigger(minute=0, timezone=BR_TZ))

    scheduler.add_job(send_shift_report, CronTrigger(hour='6,18', minute=0, timezone=BR_TZ))

    scheduler.start()
    logger.info("Agendador (Scheduler) iniciado com sucesso.")

@app.on_event("shutdown")
async def shutdown_event():
    """Executado quando a aplicação FastAPI é encerrada."""
    logger.info("Aplicação FastAPI sendo encerrada.")
    plc_connector.close_connections()

@app.get("/", tags=["Root"])
async def read_root():
    return {"message": "Bem-vindo à API de Monitoramento de Produção!"}

if __name__ == "__main__":
    logger.info("Iniciando API Core de Monitoramento e Produção...")
    logger.info(f"Host: {SERVER_HOST} | Porta: {SERVER_PORT}")
    logger.info(f"Documentação Swagger da API iterativa disponível em http://{SERVER_HOST}:{SERVER_PORT}/docs")
    uvicorn.run("src.main:app", host=SERVER_HOST, port=SERVER_PORT, reload=False)
