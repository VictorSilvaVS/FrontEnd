from pycomm3 import LogixDriver
import json
import logging
from typing import Dict, List, Optional, Any

from .config_manager import ConfigManager, MachineConfigModel

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class PLCConnector:
    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.connections: Dict[str, LogixDriver] = {} # Armazena conexões ativas {machine_name: driver}

    def _get_machine_config(self, machine_name: str) -> Optional[MachineConfigModel]:
        return self.config_manager.get_machine_config(machine_name)

    def connect(self, machine_name: str) -> Optional[LogixDriver]:
        """Estabelece ou retorna uma conexão ativa para a máquina especificada."""
        if machine_name in self.connections and self.connections[machine_name].connected:
            # logging.debug(f"Conexão com {machine_name} já está ativa.")
            return self.connections[machine_name]

        machine_config = self._get_machine_config(machine_name)
        if not machine_config:
            logging.error(f"Configuração não encontrada para a máquina: {machine_name}")
            return None

        ip = machine_config.ip_address
        slot = machine_config.processor_slot
        
        try:
            logging.info(f"Tentando conectar à máquina {machine_name} ({ip}:{slot})...")
            # LogixDriver é o driver para Allen-Bradley CompactLogix/ControlLogix
            driver = LogixDriver(ip, processor_slot=slot) 
            driver.open()
            
            if driver.connected:
                logging.info(f"Conectado com sucesso à máquina {machine_name} ({ip}:{slot}).")
                self.connections[machine_name] = driver
                return driver
            else:
                logging.error(f"Falha ao conectar à máquina {machine_name} ({ip}:{slot}). Driver não retornou conectado.")
                return None
        except Exception as e:
            logging.error(f"Erro ao conectar à máquina {machine_name} ({ip}:{slot}): {e}")
            return None

    def read_tag(self, machine_name: str, tag_key: str) -> Optional[Any]:
        """Lê um valor de tag usando a chave mapeada na configuração da máquina."""
        machine_config = self._get_machine_config(machine_name)
        if not machine_config:
            logging.error(f"Configuração não encontrada para a máquina: {machine_name}")
            return None

        plc_tag_name = machine_config.tag_mapping.get(tag_key)
        if not plc_tag_name:
            logging.warning(f"Tag key '{tag_key}' não encontrada no mapeamento da máquina {machine_name}.")
            return None
        
        driver = self.connections.get(machine_name)
        if not driver or not driver.connected:
            logging.warning(f"Conexão com {machine_name} não está ativa. Tentando reconectar...")
            driver = self.connect(machine_name)
            if not driver or not driver.connected:
                logging.error(f"Não foi possível conectar ou reconectar à máquina {machine_name}.")
                return None
        
        try:
            # `read` retorna um objeto Tag, precisamos do seu `.value`
            result = driver.read(plc_tag_name)
            if result and result.value is not None:
                # logging.debug(f"Tag '{plc_tag_name}' ({tag_key}) lida: {result.value} da máquina {machine_name}")
                return result.value
            else:
                logging.warning(f"Falha ao ler a tag PLC '{plc_tag_name}' ({tag_key}) na máquina {machine_name}. Valor retornado: {result}")
                return None
        except Exception as e:
            logging.error(f"Erro ao ler a tag PLC '{plc_tag_name}' ({tag_key}) na máquina {machine_name}: {e}")
            # Opcional: tentar fechar e reabrir a conexão se for um erro comum de comunicação
            # try:
            #     driver.close()
            #     del self.connections[machine_name]
            # except: pass
            return None

    def read_multiple_tags(self, machine_name: str, tag_keys: List[str]) -> Dict[str, Optional[Any]]:
        """Lê múltiplos valores de tags de uma vez."""
        machine_config = self._get_machine_config(machine_name)
        if not machine_config:
            logging.error(f"Configuração não encontrada para a máquina: {machine_name}")
            return {key: None for key in tag_keys}

        plc_tags_to_read = []
        key_to_plc_tag_map = {}
        for key in tag_keys:
            plc_tag = machine_config.tag_mapping.get(key)
            if plc_tag:
                plc_tags_to_read.append(plc_tag)
                key_to_plc_tag_map[plc_tag] = key # Mapeia tag PLC de volta para a chave do config
            else:
                logging.warning(f"Tag key '{key}' não encontrada no mapeamento da máquina {machine_name} para leitura múltipla.")
        
        if not plc_tags_to_read:
            return {key: None for key in tag_keys}

        driver = self.connections.get(machine_name)
        if not driver or not driver.connected:
            driver = self.connect(machine_name)
            if not driver or not driver.connected:
                logging.error(f"Não foi possível conectar ou reconectar à máquina {machine_name} para leitura múltipla.")
                return {key: None for key in tag_keys}
        
        results: Dict[str, Optional[Any]] = {key: None for key in tag_keys}
        try:
            # `read_list` retorna uma lista de objetos Tag
            read_results = driver.read_list(plc_tags_to_read)
            for tag_result in read_results:
                if tag_result and tag_result.value is not None:
                    original_key = key_to_plc_tag_map.get(tag_result.tag)
                    if original_key:
                        results[original_key] = tag_result.value
                else:
                    logging.warning(f"Falha ao ler tag PLC '{tag_result.tag}' na leitura múltipla da máquina {machine_name}.")
            return results
        except Exception as e:
            logging.error(f"Erro na leitura múltipla de tags para a máquina {machine_name}: {e}")
            return {key: None for key in tag_keys}

    def close_connections(self):
        """Fecha todas as conexões ativas."""
        for machine_name, driver in self.connections.items():
            if driver and driver.connected:
                try:
                    driver.close()
                    logging.info(f"Conexão com {machine_name} fechada.")
                except Exception as e:
                    logging.error(f"Erro ao fechar conexão com {machine_name}: {e}")
        self.connections.clear()

