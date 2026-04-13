import requests
import json
import os
import logging
from typing import Dict, Any, Optional
from datetime import datetime, timezone

class TeamsNotifier:
    def __init__(self, webhook_url: Optional[str] = None, proxies: Optional[Dict[str, str]] = None):
        self.webhook_url = webhook_url
        if not self.webhook_url:
            logging.warning("TEAMS_WEBHOOK_URL não foi fornecido na inicialização. Notificações estarão desabilitadas.")
            
        self.proxies = proxies or {}
        self.proxies = {k: v for k, v in self.proxies.items() if v}

        # Adicionado aqui para que a API possa usar
        self.adaptive_card_template_str = """
        {
            "type": "AdaptiveCard",
            "version": "1.4",
            "body": [
                {
                    "type": "ColumnSet",
                    "columns": [
                        {
                            "type": "Column",
                            "width": "auto",
                            "items": [
                                {
                                    "type": "Image",
                                    "url": "https://img.icons8.com/fluency/48/factory.png",
                                    "size": "Small"
                                }
                            ]
                        },
                        {
                            "type": "Column",
                            "width": "stretch",
                            "items": [
                                {
                                    "type": "TextBlock",
                                    "text": "REPORT DE PRODUÇÃO",
                                    "weight": "Bolder",
                                    "color": "Accent",
                                    "size": "Small",
                                    "wrap": true
                                },
                                {
                                    "type": "TextBlock",
                                    "text": "{machine_name}",
                                    "size": "ExtraLarge",
                                    "weight": "Bolder",
                                    "spacing": "None",
                                    "wrap": true
                                }
                            ]
                        }
                    ]
                },
                {
                    "type": "Container",
                    "spacing": "Medium",
                    "separator": true,
                    "style": "emphasis",
                    "items": [
                        {
                            "type": "ColumnSet",
                            "columns": [
                                {
                                    "type": "Column",
                                    "width": "stretch",
                                    "items": [
                                        {
                                            "type": "TextBlock",
                                            "text": "EFICIÊNCIA",
                                            "isSubtle": true,
                                            "weight": "Bolder",
                                            "size": "Small"
                                        },
                                        {
                                            "type": "TextBlock",
                                            "text": "{efficiency}",
                                            "color": "Good",
                                            "size": "Large",
                                            "weight": "Bolder",
                                            "spacing": "None"
                                        }
                                    ]
                                },
                                {
                                    "type": "Column",
                                    "width": "stretch",
                                    "items": [
                                        {
                                            "type": "TextBlock",
                                            "text": "PRODUÇÃO",
                                            "isSubtle": true,
                                            "weight": "Bolder",
                                            "size": "Small"
                                        },
                                        {
                                            "type": "TextBlock",
                                            "text": "{production}",
                                            "size": "Large",
                                            "weight": "Bolder",
                                            "spacing": "None"
                                        }
                                    ]
                                },
                                {
                                    "type": "Column",
                                    "width": "stretch",
                                    "items": [
                                        {
                                            "type": "TextBlock",
                                            "text": "⏳ STANDBY",
                                            "isSubtle": true,
                                            "weight": "Bolder",
                                            "size": "Small"
                                        },
                                        {
                                            "type": "TextBlock",
                                            "text": "{standby}",
                                            "size": "Large",
                                            "weight": "Bolder",
                                            "spacing": "None"
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                },
                {
                    "type": "TextBlock",
                    "text": "{footer}",
                    "isSubtle": true,
                    "size": "Small",
                    "italic": true,
                    "spacing": "Medium"
                }
            ],
            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json"
        }
        """

    def format_efficiency_value(self, value: float) -> str:
        if value is None: return "N/A"
        return f"{value:.2%}" 

    def format_production_value(self, value: int) -> str:
        if value is None: return "N/A"
        return f"{value:,}" 

    def format_standby_time(self, seconds: int) -> str:
        if seconds is None: return "N/A"
        if seconds < 0: seconds = 0 
        m, s = divmod(seconds, 60)
        h, m = divmod(m, 60)
        return f"{h:02d}:{m:02d}:{s:02d}"

    def build_card_payload(self, machine_name: str, efficiency: float, production: int, standby_seconds: int, footer: str = "") -> Dict[str, Any]:
        try:
            formatted_card_str = self.adaptive_card_template_str.format(
                machine_name=machine_name,
                efficiency=self.format_efficiency_value(efficiency),
                production=self.format_production_value(production),
                standby=self.format_standby_time(standby_seconds),
                footer=footer
            )
            payload_dict = json.loads(formatted_card_str)
            return payload_dict
        except Exception as e:
            logging.error(f"Erro ao formatar o Adaptive Card para {machine_name}: {e}")
            return {} 

    def send_message(self, card_payload: Dict[str, Any]) -> bool:
        if not self.webhook_url:
            logging.warning("TEAMS_WEBHOOK_URL não configurado. Notificações para o Teams estarão desabilitadas.")
            return False
        
        if not card_payload:
            logging.warning("Payload do cartão vazio. Não foi possível enviar a mensagem.")
            return False

        message_payload = {
            "type": "message",
            "attachments": [
                {
                    "contentType": "application/vnd.microsoft.card.adaptive",
                    "contentUrl": None,
                    "content": card_payload
                }
            ]
        }

        try:
            logging.info(f"Enviando mensagem para o Teams...")
            response = requests.post(
                self.webhook_url,
                json=message_payload,
                proxies=self.proxies,
                timeout=10 
            )
            response.raise_for_status() 
            logging.info("Mensagem enviada com sucesso para o Teams.")
            return True
        except requests.exceptions.RequestException as e:
            logging.error(f"Erro ao enviar mensagem para o Teams: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logging.error(f"Resposta do Teams (status {e.response.status_code}): {e.response.text}")
            return False
        except Exception as e:
            logging.error(f"Erro inesperado ao enviar mensagem para o Teams: {e}")
            return False

