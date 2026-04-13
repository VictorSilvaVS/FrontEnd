import os
import json
import logging
from typing import Dict, List, Optional, Any

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class MachineConfigModel:
    """Modelo simples para representar uma configuração de máquina carregada."""
    def __init__(self, config_data: Dict[str, Any]):
        self.name: str = config_data.get("name", "UnknownMachine")
        self.ip_address: str = config_data.get("ip_address")
        self.processor_slot: Optional[int] = config_data.get("processor_slot")
        self.setpoint_tag_template: Optional[str] = config_data.get("setpoint_tag_template")
        self.setpoint_structure_key: Optional[str] = config_data.get("setpoint_structure_key")
        self.tags_to_read: List[str] = config_data.get("tags_to_read", [])
        self.tag_mapping: Dict[str, str] = config_data.get("tag_mapping", {})
        self.standby_codes: List[int] = config_data.get("standby_codes", [])
        self.line_number: Optional[str] = config_data.get("line_number")

        if not self.ip_address:
            raise ValueError(f"IP address is required for machine: {self.name}")
        if not self.tag_mapping.get("status") or not self.tag_mapping.get("total_strokes"):
             raise ValueError(f"Essential tags ('status' and 'total_strokes') are missing in tag_mapping for machine: {self.name}")
        
        # Define a velocidade máxima para cálculos (prioriza max_sp, depois current_speed_spm)
        self.speed_max_tag: Optional[str] = self.tag_mapping.get("max_sp") or self.tag_mapping.get("current_speed_spm")
        if not self.speed_max_tag:
            logging.warning(f"Tag for speed_max not found in tag_mapping for machine {self.name}. Performance calculations may be affected.")


class ConfigManager:
    def __init__(self, configs_dir: str):
        self.configs_dir = configs_dir
        self.machine_configs: Dict[str, MachineConfigModel] = {} # Armazena configs carregadas {machine_name: MachineConfigModel}
        self.load_all_configs()

    def _get_config_path(self, machine_name: str) -> str:
        # Assume que o nome do arquivo é o nome da máquina com .json
        return os.path.join(self.configs_dir, f"{machine_name}.json")

    def load_config(self, machine_name: str) -> Optional[MachineConfigModel]:
        config_path = self._get_config_path(machine_name)
        if not os.path.exists(config_path):
            logging.error(f"Arquivo de configuração não encontrado: {config_path}")
            return None
        
        try:
            with open(config_path, 'r') as f:
                config_data = json.load(f)
                config_model = MachineConfigModel(config_data)
                self.machine_configs[machine_name] = config_model
                logging.info(f"Configuração carregada para a máquina: {machine_name}")
                return config_model
        except FileNotFoundError:
            logging.error(f"Arquivo de configuração não encontrado: {config_path}")
            return None
        except json.JSONDecodeError:
            logging.error(f"Erro ao decodificar o arquivo JSON: {config_path}")
            return None
        except ValueError as e:
            logging.error(f"Erro nos dados de configuração para {machine_name}: {e}")
            return None
        except Exception as e:
            logging.error(f"Erro inesperado ao carregar configuração para {machine_name}: {e}")
            return None

    def load_all_configs(self):
        if not os.path.exists(self.configs_dir):
            logging.warning(f"Diretório de configurações não encontrado: {self.configs_dir}. Nenhuma configuração será carregada.")
            return

        for filename in os.listdir(self.configs_dir):
            if filename.endswith(".json"):
                machine_name = os.path.splitext(filename)[0]
                self.load_config(machine_name)

    def get_machine_config(self, machine_name: str) -> Optional[MachineConfigModel]:
        if machine_name in self.machine_configs:
            return self.machine_configs[machine_name]
        else:
            # Tenta carregar se não estiver em cache
            return self.load_config(machine_name)

    def get_all_machine_names(self) -> List[str]:
        return list(self.machine_configs.keys())

    def get_all_configs(self) -> Dict[str, MachineConfigModel]:
        return self.machine_configs

    def update_machine_config(self, machine_name: str, new_config_data: Dict[str, Any]) -> bool:
        """Atualiza e salva a configuração de uma máquina."""
        config_path = self._get_config_path(machine_name)
        try:
            # Valida os novos dados antes de salvar
            MachineConfigModel(new_config_data) # Lança ValueError se inválido
            
            with open(config_path, 'w') as f:
                json.dump(new_config_data, f, indent=4)
            
            # Atualiza o cache interno
            self.machine_configs[machine_name] = MachineConfigModel(new_config_data)
            logging.info(f"Configuração atualizada e salva para: {machine_name}")
            return True
        except ValueError as e:
            logging.error(f"Erro nos dados fornecidos para atualização de {machine_name}: {e}")
            return False
        except Exception as e:
            logging.error(f"Erro ao salvar configuração para {machine_name}: {e}")
            return False

