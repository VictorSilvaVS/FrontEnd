from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime

class MachineDataRaw(BaseModel):
    machine_name: str
    ign_status: Optional[int] = None
    pv_shift_stroke_count: Optional[int] = None
    machine_state: Optional[int] = None
    timestamp: Optional[datetime] = Field(default_factory=datetime.now)

class MachineDataDB(BaseModel):
    id: int
    timestamp: datetime
    machine_name: str
    ign_status: Optional[int] = None
    pv_shift_stroke_count: Optional[int] = None
    machine_state: Optional[int] = None
    total_run_time_seconds: int
    standby_time_seconds: int

class EfficiencyMetrics(BaseModel):
    machine_name: str
    period_start: str
    period_end: str
    data_points_count: int
    total_time_seconds: float
    scheduled_time_seconds: float
    available_time_seconds: float
    operating_time_seconds: float
    downtime_seconds: float
    standby_downtime_seconds: float
    total_strokes: int
    ideal_strokes_per_second: float
    performance_ratio: float
    availability_ratio: float
    quality_ratio: float
    oee: float
    average_strokes_per_second: float
    average_operating_speed_value: float
    recorded_standby_codes: List[int]

class MachineConfig(BaseModel):
    name: str
    ip_address: str
    protocol: str = "ethernetip"
    tags: dict

class PlcConfig(BaseModel):
    machines: List[MachineConfig]
