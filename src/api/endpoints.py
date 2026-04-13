from fastapi import APIRouter, HTTPException, Depends, Query
from typing import List, Dict, Optional
from datetime import datetime, timedelta
import json

from .models import MachineDataDB, MachineDataRaw, EfficiencyMetrics, PlcConfig, MachineConfig
from src.database import Database
from src.calculations import EfficiencyCalculator
from src.plc_connector import PLCConnector

router = APIRouter()

db = None
calculator = None
plc_connector = None
config_manager = None

def set_dependencies(database, efficiency_calculator, connector, cfg_manager=None):
    """Injeta as dependências necessárias."""
    global db, calculator, plc_connector, config_manager
    db = database
    calculator = efficiency_calculator
    plc_connector = connector
    config_manager = cfg_manager

@router.get("/config/machines", summary="Lista configurações de todas as máquinas")
def get_all_machines_config():
    return config_manager.get_all_configs()

@router.post("/config/machine/{machine_name}", summary="Cria ou atualiza configuração de uma máquina")
def update_machine_config(machine_name: str, config: Dict[str, Any]):
    success = config_manager.update_machine_config(machine_name, config)
    if not success:
        raise HTTPException(status_code=400, detail="Erro ao salvar configuração da máquina.")
    return {"message": f"Máquina {machine_name} configurada com sucesso."}

import os

@router.get("/config/standby_codes", summary="Lista códigos de standby globais")
def get_global_standby_codes():
    file_path = os.environ["STANDBY_CODES_FILE"]
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except:
        return {"standby_codes": []}

@router.post("/config/standby_codes", summary="Atualiza códigos de standby globais")
def update_global_standby_codes(data: Dict[str, List[int]]):
    file_path = os.environ["STANDBY_CODES_FILE"]
    try:
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=4)
        return {"message": "Códigos de standby atualizados."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao salvar códigos: {e}")

@router.get("/data/{machine_name}", response_model=List[MachineDataDB], summary="Obtém dados brutos recentes de uma máquina")
def get_recent_machine_data(machine_name: str, limit: int = 50):
    data = db.get_recent_data(machine_name, limit=limit)
    if not data:
        raise HTTPException(status_code=404, detail=f"Dados não encontrados para a máquina {machine_name} ou máquina não configurada.")

    return [MachineDataDB(**item) for item in data]

@router.get("/hourly/{machine_name}", summary="Obtém consolidados horários de uma máquina")
def get_hourly_metrics(
    machine_name: str,
    start_time: str = Query(..., description="ISO 8601"),
    end_time: str = Query(..., description="ISO 8601")
):
    try:
        st = datetime.fromisoformat(start_time)
        et = datetime.fromisoformat(end_time)
        return db.get_hourly_data(machine_name, st, et)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
