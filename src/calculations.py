from datetime import datetime, timedelta, timezone
import json
import logging
from typing import List, Dict, Any, Optional

from .database import Database
from .config_manager import ConfigManager, MachineConfigModel

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class EfficiencyCalculator:
    def __init__(self, db_path: str, config_manager: ConfigManager):
        self.db_path = db_path
        self.config_manager = config_manager

    def get_standby_codes_for_machine(self, machine_name: str) -> List[int]:
        """Retorna os códigos de standby específicos para a máquina ou uma lista vazia."""
        config = self.config_manager.get_machine_config(machine_name)
        if config:
            return config.standby_codes
        return []

    def calculate_metrics_for_period(self, machine_name: str, data_points: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Calcula métricas de eficiência para um período, usando dados brutos
        que JÁ DEVEM TER OS CAMPOS DE INTERVALO CALULADOS (`interval_run_time_seconds`, `interval_standby_time_seconds`).
        """
        if not data_points:
            return self._get_empty_metrics_dict(machine_name, "No data points provided")

        data_points.sort(key=lambda x: datetime.fromisoformat(x['timestamp']))

        first_record = data_points[0]
        last_record = data_points[-1]

        period_start_dt = datetime.fromisoformat(first_record['timestamp']).replace(tzinfo=timezone.utc)
        period_end_dt = datetime.fromisoformat(last_record['timestamp']).replace(tzinfo=timezone.utc)

        total_duration_seconds = (period_end_dt - period_start_dt).total_seconds()
        if total_duration_seconds <= 0 and len(data_points) > 1:
             total_duration_seconds = 60
        elif total_duration_seconds <= 0 and len(data_points) == 1:
             total_duration_seconds = 0

        scheduled_time_seconds = total_duration_seconds

        total_strokes_in_period = 0
        total_standby_downtime_seconds = 0
        total_operating_time_seconds = 0
        total_cycle_time_seconds = 0

        standby_codes = self.get_standby_codes_for_machine(machine_name)

        for record in data_points:
            interval_run_time = record.get('interval_run_time_seconds', 0)
            interval_standby_time = record.get('interval_standby_time_seconds', 0)

            total_cycle_time_seconds += interval_run_time

            current_status = record.get('status')
            if current_status in standby_codes:
                total_standby_downtime_seconds += interval_run_time

            if interval_run_time > 0 and current_status not in standby_codes and current_status is not None:

                 total_operating_time_seconds += interval_run_time

        current_total_strokes_raw = 0
        if data_points:

             current_total_strokes_raw = data_points[-1].get('total_strokes', 0)

             total_strokes_in_period = 0
             previous_stroke_count = None
             for record in data_points:
                  strokes = record.get('total_strokes')
                  if strokes is not None:
                       if previous_stroke_count is None:
                            previous_stroke_count = strokes
                       else:
                            strokes_diff = strokes - previous_stroke_count

                            if strokes_diff < 0:

                                 if strokes > 0:

                                     pass

                            if strokes_diff >= 0:
                                total_strokes_in_period += strokes_diff

                            previous_stroke_count = strokes

        available_time_seconds = max(0, scheduled_time_seconds - total_standby_downtime_seconds)
        availability_ratio = (available_time_seconds / scheduled_time_seconds) if scheduled_time_seconds > 0 else 0.0

        config = self.config_manager.get_machine_config(machine_name)
        speed_max_tag = config.speed_max_tag if config else None
        speed_max_value = 0.0

        max_sp_values = [r.get('max_sp') for r in data_points if r.get('max_sp') is not None]
        current_spm_values = [r.get('current_speed_spm') for r in data_points if r.get('current_speed_spm') is not None]

        if max_sp_values:

            speed_max_value = max_sp_values[-1]
        elif current_spm_values:

            speed_max_value = current_spm_values[-1]
        else:
            logging.warning(f"Nenhum valor válido para velocidade máxima (max_sp ou current_speed_spm) encontrado para {machine_name} no período.")
            speed_max_value = 100.0

        speed_max_sps = speed_max_value / 60.0 if speed_max_value > 0 else 0.0

