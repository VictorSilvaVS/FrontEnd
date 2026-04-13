from fastapi import APIRouter, HTTPException, Depends, Query
from typing import List, Dict, Optional
from datetime import datetime, timedelta
import json

from .models import MachineDataDB, MachineDataRaw, EfficiencyMetrics, PlcConfig, MachineConfig # Importa os modelos Pydantic
from src.database import Database
from src.calculations import EfficiencyCalculator
from src.plc_connector import PLCConnector # Necessário para simular coleta ou para uma API de "trigger"

router = APIRouter()

# --- Dependências ---
# Instanciações globais (ou gerenciadas por um framework de DI se necessário)
# Estas serão inicializadas pelo main.py e injetadas se necessário
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

# --- Rotas de Configuração (Totalmente via Web) ---

@router.get("/config/machines", summary="Lista configurações de todas as máquinas")
def get_all_machines_config():
    return config_manager.get_all_configs()

@router.post("/config/machine/{machine_name}", summary="Cria ou atualiza configuração de uma máquina")
def update_machine_config(machine_name: str, config: Dict[str, Any]):
    success = config_manager.update_machine_config(machine_name, config)
    if not success:
        raise HTTPException(status_code=400, detail="Erro ao salvar configuração da máquina.")
    return {"message": f"Máquina {machine_name} configurada com sucesso."}

@router.get("/config/standby_codes", summary="Lista códigos de standby globais")
def get_global_standby_codes():
    try:
        with open("configs/standby_codes.json", 'r') as f:
            return json.load(f)
    except:
        return {"standby_codes": []}

@router.post("/config/standby_codes", summary="Atualiza códigos de standby globais")
def update_global_standby_codes(data: Dict[str, List[int]]):
    try:
        with open("configs/standby_codes.json", 'w') as f:
            json.dump(data, f, indent=4)
        return {"message": "Códigos de standby atualizados."}
    except:
        raise HTTPException(status_code=500, detail="Erro ao salvar códigos.")

# --- Rotas de API ---

@router.get("/machines", summary="Lista todas as máquinas configuradas")
def list_machines():
    plc_configs = get_plc_config()
    return {"machines": plc_configs.get("machines", [])}

@router.put("/machines", summary="Atualiza a configuração de máquinas")
def update_machines_config(new_plc_config: PlcConfig):
    try:
        with open("configs/plc_config.json", 'w') as f:
            json.dump(new_plc_config.dict(), f, indent=4)
        # Você pode querer reiniciar ou recarregar o conector do PLC aqui
        # plc_connector.configs = new_plc_config.machines # Atualiza a instância do conector
        return {"message": "Configuração de máquinas atualizada com sucesso."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao salvar configs/plc_config.json: {e}")

@router.get("/standby_codes", summary="Lista os códigos de standby")
def get_standby_codes_endpoint():
    return {"standby_codes": get_standby_codes()}

@router.put("/standby_codes", summary="Atualiza os códigos de standby")
def update_standby_codes(codes: Dict[str, List[int]]):
    try:
        with open("configs/standby_codes.json", 'w') as f:
            json.dump(codes, f, indent=4)
        # Atualiza os códigos no calculator se ele estiver carregado
        # calculator.standby_codes = set(codes.get("standby_codes", [])) # Se o calculator tiver um método para isso
        return {"message": "Códigos de standby atualizados com sucesso."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao salvar configs/standby_codes.json: {e}")

@router.get("/data/{machine_name}", response_model=List[MachineDataDB], summary="Obtém dados brutos recentes de uma máquina")
def get_recent_machine_data(machine_name: str, limit: int = 50):
    data = db.get_recent_data(machine_name, limit=limit)
    if not data:
        raise HTTPException(status_code=404, detail=f"Dados não encontrados para a máquina {machine_name} ou máquina não configurada.")
    
    # Converte dicionários para modelos Pydantic (se necessário para serialização formal)
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
