from dataclasses import dataclass
from datetime import datetime

@dataclass
class recieve_data:
    user_id: str
    session_id: str
    ppg_value: float
    date: datetime
    
@dataclass
class send_data:
    user_id: str
    session_id: str
    focus_rate: float
    level: int
    date: datetime
    
