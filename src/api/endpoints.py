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
plc_connector = None # Para gerenciar conexões de forma mais robusta

def set_dependencies(database, efficiency_calculator, connector):
    """Injeta as dependências necessárias."""
    global db, calculator, plc_connector
    db = database
    calculator = efficiency_calculator
    plc_connector = connector

# Simula a leitura de configurações de PLC e Standby para a API
def get_plc_config():
    try:
        with open("configs/plc_config.json", 'r') as f:
            return json.load(f)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao carregar configs/plc_config.json: {e}")

def get_standby_codes():
    try:
        with open("configs/standby_codes.json", 'r') as f:
            config = json.load(f)
            return config.get("standby_codes", [])
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao carregar configs/standby_codes.json: {e}")

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

@router.get("/metrics/{machine_name}", response_model=EfficiencyMetrics, summary="Calcula e retorna métricas de eficiência para um período")
def get_efficiency_metrics(
    machine_name: str,
    start_time: str = Query(..., description="Tempo de início no formato ISO 8601 (YYYY-MM-DDTHH:MM:SS)"),
    end_time: str = Query(..., description="Tempo de fim no formato ISO 8601 (YYYY-MM-DDTHH:MM:SS)")
):
    try:
        # Valida o formato das datas antes de chamar o calculator
        datetime.fromisoformat(start_time)
        datetime.fromisoformat(end_time)
    except ValueError:
        raise HTTPException(status_code=400)
