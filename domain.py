from dataclasses import dataclass
from datetime import datetime

@dataclass
class ppg_data:
    user_id: str | int = None
    session_id: str = None
    ppg_value: float = 0.0
    time: datetime = None
    
@dataclass
class focus_data:
    user_id: str | int = None
    session_id: str = None
    focus_rate: float = 0.0
    level: int = 0
    time: datetime = None
    
