from datetime import datetime, timedelta, timezone
import json
import logging
from typing import List, Dict, Any, Optional

# Importa dependências locais
from .database import Database
from .config_manager import ConfigManager, MachineConfigModel

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class EfficiencyCalculator:
    def __init__(self, db_path: str = "database/production_data.db", config_manager: ConfigManager = None):
        self.db_path = db_path
        self.config_manager = config_manager
        if not self.config_manager:
            self.config_manager = ConfigManager() # Instancia se não for passada
            logging.warning("ConfigManager não foi passado para EfficiencyCalculator, instanciando um novo.")

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

        # Garante que os dados estejam ordenados por timestamp
        data_points.sort(key=lambda x: datetime.fromisoformat(x['timestamp']))

        first_record = data_points[0]
        last_record = data_points[-1]
        
        period_start_dt = datetime.fromisoformat(first_record['timestamp']).replace(tzinfo=timezone.utc)
        period_end_dt = datetime.fromisoformat(last_record['timestamp']).replace(tzinfo=timezone.utc)
        
        total_duration_seconds = (period_end_dt - period_start_dt).total_seconds()
        if total_duration_seconds <= 0 and len(data_points) > 1: # Se houver mais de um ponto, mas a duração é zero ou negativa (erro)
             total_duration_seconds = 60 # Valor padrão para um intervalo mínimo
        elif total_duration_seconds <= 0 and len(data_points) == 1: # Apenas um ponto, a duração é incerta. Podemos usar um intervalo padrão ou zero.
             total_duration_seconds = 0 # Ou 60 para simular um minuto de coleta

        # Assume que o tempo programado é igual à duração total observada
        scheduled_time_seconds = total_duration_seconds 

        total_strokes_in_period = 0
        total_standby_downtime_seconds = 0
        total_operating_time_seconds = 0 # Tempo onde status indica operação ativa
        total_cycle_time_seconds = 0 # Tempo total que a máquina esteve rodando (sem contar paradas SC)

        # Obtém os códigos de standby específicos para esta máquina
        standby_codes = self.get_standby_codes_for_machine(machine_name)

        # Itera sobre os registros para somar os tempos de intervalo
        for record in data_points:
            interval_run_time = record.get('interval_run_time_seconds', 0)
            interval_standby_time = record.get('interval_standby_time_seconds', 0)
            
            total_cycle_time_seconds += interval_run_time

            # Verifica se o status atual é de standby e adiciona o tempo de parada
            current_status = record.get('status')
            if current_status in standby_codes:
                total_standby_downtime_seconds += interval_run_time # O tempo total do intervalo foi standby
            
            # Calcula o tempo operacional (quando IGN.Status > 0, por exemplo)
            # Assumindo que um status diferente de standby E diferente de 0 é tempo operacional
            if interval_run_time > 0 and current_status not in standby_codes and current_status is not None:
                 # Considera tempo operacional se não for standby e o status for válido
                 # Poderíamos refinar: se status == 1 (operando), etc.
                 total_operating_time_seconds += interval_run_time
            
            # Soma os strokes. Precisamos da diferença entre registros consecutivos.
            # Se `total_strokes` for um contador que reseta, essa lógica precisa ser mais complexa.
            # Para este exemplo, assumimos que `total_strokes` na tabela é o valor bruto lido.
            # O cálculo de `total_strokes_in_period` deve ser feito em `main.py` ou em um passo anterior
            # comparando o `total_strokes` do registro atual com o anterior.
            # Aqui, vamos usar a contagem total se disponível, ou assumir que o cálculo
            # de diferença já foi feito nos campos `interval_run_time_seconds` de alguma forma.
            # **Melhor abordagem**: Calcular a diferença de `total_strokes` entre registros consecutivos
            # E somar essa diferença para obter `total_strokes_in_period`.
            # Vamos fazer isso agora.

        # Recalcula total_strokes_in_period com base nas diferenças
        current_total_strokes_raw = 0
        if data_points:
             # Pega o último valor lido de `total_strokes`
             current_total_strokes_raw = data_points[-1].get('total_strokes', 0)
             # Para obter o total no período, precisaríamos do valor *antes* do primeiro registro,
             # ou iterar e somar as diferenças. A iteração é mais segura.
             total_strokes_in_period = 0
             previous_stroke_count = None
             for record in data_points:
                  strokes = record.get('total_strokes')
                  if strokes is not None:
                       if previous_stroke_count is None: # Primeiro registro com valor
                            previous_stroke_count = strokes
                       else:
                            strokes_diff = strokes - previous_stroke_count
                            # Lida com reset do contador do PLC (se o novo valor for menor que o anterior)
                            if strokes_diff < 0:
                                 # Assumindo que o reset ocorreu e o valor é do início do ciclo
                                 # Precisamos saber o valor máximo possível para calcular o wrap-around,
                                 # ou simplesmente considerar os strokes desde o último *reset*.
                                 # Uma heurística simples é: se a diferença é negativa, é um reset.
                                 # A quantidade de strokes adicionados desde o reset pode ser estimada,
                                 # mas sem saber o MAX_VALUE do contador, é complicado.
                                 # Para simplificar: se for negativo, assumimos que o contador reiniciou
                                 # e apenas adicionamos o valor atual (se ele não for zero).
                                 # Uma abordagem mais robusta seria armazenar o `total_strokes` *antes* do primeiro registro
                                 # (o último do período anterior) e usar isso.
                                 # Vamos assumir para agora que a diferença negativa indica que o contador reiniciou e
                                 # não estamos perdendo contagem significativa para o período analisado se o período for curto.
                                 # Se for um período longo e o reset for comum, isso precisa ser melhor tratado.
                                 # Por ora, vamos apenas pegar a diferença e tratar resete simples.
                                 if strokes > 0: # Se o contador resetou e o novo valor é maior que 0
                                     # Uma aproximação seria adicionar o valor atual + (MAX_VAL - previous_stroke_count)
                                     # Mas sem MAX_VAL, vamos apenas assumir que o contador atual é o novo ponto de partida.
                                     # Se você tiver um tag para o valor máximo do contador, use-o aqui.
                                     # Para simplicidade, vamos adicionar a diferença `strokes - previous_stroke_count`
                                     # e se for negativo, o `total_strokes_in_period` pode ficar impreciso para aquele intervalo.
                                     # Se a diferença for muito grande e negativa, pode ser um erro de leitura ou reset.
                                     # Tratamento básico: Se a diferença for < 0, assumimos que o contador resetou e somamos apenas o valor atual
                                     # Se o valor atual for maior que o anterior, somamos a diferença.
                                     # Essa é uma simplificação!
                                     pass # Não adicionamos nada se for negativo, pois o `previous_stroke_count` será atualizado.
                                 # Se strokes_diff é negativo, o `previous_stroke_count` será atualizado para `strokes`
                                 # e a próxima diferença será calculada a partir daí.
                                 # Para um cálculo preciso, seria melhor registrar o `total_strokes` do último ponto
                                 # *antes* do início do período analisado.

                            if strokes_diff >= 0:
                                total_strokes_in_period += strokes_diff
                            
                            previous_stroke_count = strokes # Atualiza para o próximo cálculo

        # --- Cálculo das Métricas Principais ---
        
        # 1. Disponibilidade
        # Tempo Disponível = Tempo Programado - Tempo de Parada (SC)
        # Tempo de Parada (SC) = total_standby_downtime_seconds
        available_time_seconds = max(0, scheduled_time_seconds - total_standby_downtime_seconds)
        availability_ratio = (available_time_seconds / scheduled_time_seconds) if scheduled_time_seconds > 0 else 0.0

        # 2. Performance
        # Precisamos da velocidade máxima (speed_max) para calcular a produção ideal.
        # Vamos buscar a tag `max_sp` da configuração da máquina.
        config = self.config_manager.get_machine_config(machine_name)
        speed_max_tag = config.speed_max_tag if config else None
        speed_max_value = 0.0 # Default: 0 SPM (ou Strokes por Minuto)
        
        # Nota: Precisamos ler o valor da tag `speed_max_tag` em tempo real ou usar um valor configurado.
        # Para este exemplo, vamos usar um valor de `max_sp` encontrado nos dados ou um default.
        # Se `max_sp` varia, o cálculo de performance se torna mais complexo.
        # Vamos tentar encontrar um valor válido de `max_sp` nos dados, ou usar o último `current_speed_spm` se `max_sp` não estiver disponível.
        
        max_sp_values = [r.get('max_sp') for r in data_points if r.get('max_sp') is not None]
        current_spm_values = [r.get('current_speed_spm') for r in data_points if r.get('current_speed_spm') is not None]

        if max_sp_values:
            # Usamos a média ou o último valor de `max_sp` se ele for estável.
            # Assumindo `max_sp` é um valor de setpoint que pode ser lido.
            # Para simplificar, vamos pegar o último valor válido de `max_sp`.
            speed_max_value = max_sp_values[-1] 
        elif current_spm_values:
            # Se `max_sp` não estiver disponível, usamos `current_speed_spm` como uma referência de velocidade.
            # Isso é menos ideal, pois `current_speed_spm` é a velocidade *real*, não a *ideal*.
            speed_max_value = current_spm_values[-1] 
        else:
            logging.warning(f"Nenhum valor válido para velocidade máxima (max_sp ou current_speed_spm) encontrado para {machine_name} no período.")
            speed_max_value = 100.0 # Um valor padrão genérico, mas impreciso. AJUSTE ISSO.

        # Precisamos converter SPM (Strokes Per Minute) para Strokes Per Second (SPS)
        speed_max_sps = speed_max_value / 60.0 if speed_max_value > 0 else 0.0

        # Produção Ideal = Velocidade Máxima (SPS) * Tempo Operacional Efetivo (segundos)
        # O "Tempo Operacional Efetivo" para performance é o tempo em que a máquina *deveria* estar produzindo.
        # Em OEE, isso geralmente é o Tempo Disponível (Scheduled Time - Downtime SCs).
        # Vamos usar `available_time_seconds` como base.
        
        # A forma mais comum de calcular performance no OEE é:
        # Performance = Produção Real / Produção Ideal
        # Produção Real = `total_strokes_in_period`
        # Produção Ideal = `speed_max_sps` * `available_time_seconds` (Tempo em que a máquina *poderia* ter produzido)
        
        # Alternativa: Produção Ideal = `speed_max_sps` * `total_cycle_time_seconds` (Tempo total rodando, sem SCs)
        # A escolha depende da sua definição de OEE. Vamos usar `available_time_seconds
