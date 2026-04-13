import sqlite3
import json
from datetime import datetime, timezone
import logging
from typing import List, Dict, Any, Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Database:
    def __init__(self, db_path: str, config_manager=None):
        self.db_path = db_path
        self.config_manager = config_manager
        self._create_tables()

    def set_config_manager(self, config_manager):
        self.config_manager = config_manager

    def _create_tables(self):
        """Garante que as tabelas necessárias existam no banco de dados."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS machine_data (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp DATETIME NOT NULL,
                        machine_name TEXT NOT NULL,
                        status INTEGER,
                        total_strokes INTEGER,
                        current_speed_spm INTEGER,
                        max_sp REAL,
                        min_sp REAL,
                        interval_run_time_seconds INTEGER DEFAULT 0,
                        interval_standby_time_seconds INTEGER DEFAULT 0,
                        UNIQUE(timestamp, machine_name)
                    )
                ''')

                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS hourly_data (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        machine_name TEXT NOT NULL,
                        hour_timestamp DATETIME NOT NULL, -- Ex: 2024-04-13 14:00:00
                        total_production INTEGER DEFAULT 0,
                        run_time_seconds INTEGER DEFAULT 0,
                        standby_time_seconds INTEGER DEFAULT 0,
                        availability REAL DEFAULT 0.0,
                        performance REAL DEFAULT 0.0,
                        oee REAL DEFAULT 0.0,
                        UNIQUE(machine_name, hour_timestamp)
                    )
                ''')
                conn.commit()
            logging.info(f"Tabela 'machine_data' verificada/criada com sucesso em {self.db_path}")
        except sqlite3.Error as e:
            logging.error(f"Erro ao criar tabelas no banco de dados: {e}")

            raise

    def insert_data_batch(self, data_list: List[Dict[str, Any]]):
        """Insere uma lista de dicionários de dados em lote."""
        if not data_list:
            return 0

        rows_inserted = 0
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                records_to_insert = []
                for data in data_list:
                    timestamp = datetime.now(timezone.utc)
                    machine_name = data.get("machine_name")

                    if not machine_name:
                        logging.warning("Registro ignorado: machine_name ausente.")
                        continue

                    record = {
                        "timestamp": timestamp,
                        "machine_name": machine_name,
                        "status": data.get("status"),
                        "total_strokes": data.get("total_strokes"),
                        "current_speed_spm": data.get("current_speed_spm"),
                        "max_sp": data.get("max_sp"),
                        "min_sp": data.get("min_sp"),

                        "interval_run_time_seconds": 0,
                        "interval_standby_time_seconds": 0
                    }
                    records_to_insert.append(record)

                cols = list(records_to_insert[0].keys())
                placeholders = ', '.join('?' * len(cols))
                sql = f"INSERT INTO machine_data ({', '.join(cols)}) VALUES ({placeholders})"

                cursor.executemany(sql, [tuple(r[col] for col in cols) for r in records_to_insert])
                conn.commit()
                rows_inserted = len(records_to_insert)
                logging.info(f"{rows_inserted} registros inseridos com sucesso.")
        except sqlite3.IntegrityError:
            logging.warning("Duplicidade encontrada. Alguns registros podem não ter sido inseridos.")
        except sqlite3.Error as e:
            logging.error(f"Erro ao inserir dados no banco de dados: {e}")

        return rows_inserted

    def get_last_record(self, machine_name: str) -> Optional[Dict[str, Any]]:
        """Busca o último registro inserido para uma máquina específica."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT * FROM machine_data
                    WHERE machine_name = ?
                    ORDER BY timestamp DESC LIMIT 1
                """, (machine_name,))
                row = cursor.fetchone()
                if row:
                    columns = [description[0] for description in cursor.description]
                    return dict(zip(columns, row))
            return None
        except sqlite3.Error as e:
            logging.error(f"Erro ao buscar último registro para {machine_name}: {e}")
            return None

    def get_data_for_period(self, start_time: datetime, end_time: datetime, machine_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """Busca dados brutos para um período e máquina específicos, ordenados por timestamp."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                query = "SELECT * FROM machine_data WHERE timestamp BETWEEN ? AND ?"
                params = [start_time.isoformat(), end_time.isoformat()]
                if machine_name:
                    query += " AND machine_name = ?"
                    params.append(machine_name)
                query += " ORDER BY timestamp ASC"

                cursor.execute(query, params)
                rows = cursor.fetchall()
                columns = [description[0] for description in cursor.description]
                return [dict(zip(columns, row)) for row in rows]
        except sqlite3.Error as e:
            logging.error(f"Erro ao buscar dados para o período: {e}")
            return []

    def update_interval_times(self, records: List[Dict[str, Any]]):
        """
        Calcula e atualiza os campos 'interval_run_time_seconds' e 'interval_standby_time_seconds'
        para uma lista de registros de uma máquina, que devem estar ordenados por timestamp.
        """
        if not records:
            return

        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                machine_data_grouped: Dict[str, List[Dict[str, Any]]] = {}
                for record in records:
                    machine = record['machine_name']
                    if machine not in machine_data_grouped:
                        machine_data_grouped[machine] = []
                    machine_data_grouped[machine].append(record)

                for machine, data_list in machine_data_grouped.items():

                    data_list.sort(key=lambda x: datetime.fromisoformat(x['timestamp']))

                    for i in range(len(data_list)):
                        current_record = data_list[i]
                        current_id = current_record['id']
                        current_timestamp = datetime.fromisoformat(current_record['timestamp'])

                        interval_run_time = 0
                        interval_standby_time = 0

                        if i > 0:
                            prev_record = data_list[i-1]
                            prev_timestamp = datetime.fromisoformat(prev_record['timestamp'])
                            time_diff_seconds = int((current_timestamp - prev_timestamp).total_seconds())

                            if time_diff_seconds > 0:
                                interval_run_time = time_diff_seconds

                                current_status = current_record.get('status')

                                machine_config = self.config_manager.get_machine_config(machine)
                                if machine_config and current_status in machine_config.standby_codes:
                                     interval_standby_time = interval_run_time

                        cursor.execute("""
                            UPDATE machine_data
                            SET interval_run_time_seconds = ?, interval_standby_time_seconds = ?
                            WHERE id = ?
                        """, (interval_run_time, interval_standby_time, current_id))

                conn.commit()
                logging.info(f"Tempos de intervalo atualizados para {len(records)} registros.")
        except sqlite3.Error as e:
            logging.error(f"Erro ao atualizar tempos de intervalo: {e}")
            if conn: conn.rollback()
        except Exception as e:
             logging.error(f"Erro inesperado ao atualizar tempos de intervalo: {e}")
             if conn: conn.rollback()
    def insert_hourly_rollup(self, rollup_data: Dict[str, Any]):
        """Insere ou atualiza um consolidado horário."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cols = list(rollup_data.keys())
                placeholders = ', '.join('?' * len(cols))

                sql = f"INSERT OR REPLACE INTO hourly_data ({', '.join(cols)}) VALUES ({placeholders})"
                cursor.execute(sql, tuple(rollup_data[col] for col in cols))
                conn.commit()
        except sqlite3.Error as e:
            logging.error(f"Erro ao inserir rollup horário: {e}")

    def get_hourly_data(self, machine_name: str, start_time: datetime, end_time: datetime) -> List[Dict[str, Any]]:
        """Busca dados consolidados horários."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT * FROM hourly_data
                    WHERE machine_name = ? AND hour_timestamp BETWEEN ? AND ?
                    ORDER BY hour_timestamp ASC
                """, (machine_name, start_time.isoformat(), end_time.isoformat()))
                rows = cursor.fetchall()
                columns = [description[0] for description in cursor.description]
                return [dict(zip(columns, row)) for row in rows]
        except sqlite3.Error as e:
            logging.error(f"Erro ao buscar dados horários: {e}")
            return []
