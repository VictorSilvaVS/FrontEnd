import sqlite3
import json
from datetime import datetime, timezone
import logging
from typing import List, Dict, Any, Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Database:
    def __init__(self, db_path="database/production_data.db"):
        self.db_path = db_path
        self._create_tables()

    def _create_tables(self):
        """Garante que as tabelas necessárias existam no banco de dados."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                # Ajuste as colunas para refletir os nomes usados no mapeamento mais comum
                # Usaremos nomes mais genéricos que são mapeados a partir do JSON
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS machine_data (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp DATETIME NOT NULL,
                        machine_name TEXT NOT NULL,
                        status INTEGER,                -- Mapeado de 'status' (e.g., IGN.Status)
                        total_strokes INTEGER,         -- Mapeado de 'total_strokes' (e.g., oPV_Shift_Stroke_Count)
                        current_speed_spm INTEGER,     -- Mapeado de 'current_speed_spm' (e.g., Machine_Speed_SPM)
                        -- Outras tags importantes que você queira salvar:
                        max_sp REAL,                   -- Mapeado de 'max_sp'
                        min_sp REAL,                   -- Mapeado de 'min_sp'
                        -- Campos calculados (para otimizar consultas de eficiência)
                        interval_run_time_seconds INTEGER DEFAULT 0, -- Tempo decorrido desde o último registro para esta máquina
                        interval_standby_time_seconds INTEGER DEFAULT 0, -- Porção do interval_run_time_seconds que foi standby (SC)
                        UNIQUE(timestamp, machine_name) -- Evita duplicidade exata, se timestamp for preciso
                    )
                ''')
                conn.commit()
            logging.info(f"Tabela 'machine_data' verificada/criada com sucesso em {self.db_path}")
        except sqlite3.Error as e:
            logging.error(f"Erro ao criar tabelas no banco de dados: {e}")
            # Se falhar aqui, o aplicativo não poderá operar corretamente
            raise

    def insert_data_batch(self, data_list: List[Dict[str, Any]]):
        """Insere uma lista de dicionários de dados em lote."""
        if not data_list:
            return 0

        rows_inserted = 0
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Prepara os dados para inserção
                # Cada item em data_list deve ser um dicionário com as chaves correspondentes à tabela
                # O timestamp deve ser incluído ou gerado aqui
                
                records_to_insert = []
                for data in data_list:
                    timestamp = datetime.now(timezone.utc) # Usar UTC é uma boa prática
                    machine_name = data.get("machine_name")

                    if not machine_name:
                        logging.warning("Registro ignorado: machine_name ausente.")
                        continue

                    # Garante que todos os campos da tabela tenham um valor (mesmo que None)
                    record = {
                        "timestamp": timestamp,
                        "machine_name": machine_name,
                        "status": data.get("status"),
                        "total_strokes": data.get("total_strokes"),
                        "current_speed_spm": data.get("current_speed_spm"),
                        "max_sp": data.get("max_sp"),
                        "min_sp": data.get("min_sp"),
                        # Campos calculados iniciam em 0
                        "interval_run_time_seconds": 0, 
                        "interval_standby_time_seconds": 0
                    }
                    records_to_insert.append(record)

                # Executa a inserção em lote
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
            # O rollback é implícito ao sair do bloco 'with' em caso de erro
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
                query += " ORDER BY timestamp ASC" # Essencial para cálculos de intervalo

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
                
                # Agrupa por máquina caso a lista contenha dados de várias
                machine_data_grouped: Dict[str, List[Dict[str, Any]]] = {}
                for record in records:
                    machine = record['machine_name']
                    if machine not in machine_data_grouped:
                        machine_data_grouped[machine] = []
                    machine_data_grouped[machine].append(record)

                # Processa cada máquina separadamente
                for machine, data_list in machine_data_grouped.items():
                    # Garante que a lista esteja ordenada por timestamp
                    data_list.sort(key=lambda x: datetime.fromisoformat(x['timestamp']))

                    for i in range(len(data_list)):
                        current_record = data_list[i]
                        current_id = current_record['id']
                        current_timestamp = datetime.fromisoformat(current_record['timestamp'])

                        interval_run_time = 0
                        interval_standby_time = 0
                        
                        # Calcula a diferença com o registro anterior para obter o tempo do intervalo
                        if i > 0:
                            prev_record = data_list[i-1]
                            prev_timestamp = datetime.fromisoformat(prev_record['timestamp'])
                            time_diff_seconds = int((current_timestamp - prev_timestamp).total_seconds())
                            
                            if time_diff_seconds > 0:
                                interval_run_time = time_diff_seconds
                                
                                # Verifica se o estado *atual* é um código de standby
                                current_status = current_record.get('status')
                                # Precisamos dos standby_codes da configuração da máquina
                                machine_config = self.config_manager.get_machine_config(machine) # Assumindo que config_manager é acessível
                                if machine_config and current_status in machine_config.standby_codes:
                                     interval_standby_time = interval_run_time # O intervalo completo foi em standby

                        # Atualiza o registro no banco de dados
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
        except Exception as e: # Captura erros de config_manager, etc.
             logging.error(f"Erro inesperado ao atualizar tempos de intervalo: {e}")
             if conn: conn.rollback()
