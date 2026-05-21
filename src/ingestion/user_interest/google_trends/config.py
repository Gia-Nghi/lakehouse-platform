from dataclasses import dataclass, field
from typing import Dict, List


@dataclass
class GoogleTrendsConfig:
    geo: str = "VN"
    target_region: str = "Vietnam"

    initial_timeframe: str = "today 12-m"
    daily_timeframe: str = "today 1-m"

    months_back: int = 12
    window_days: int = 30

    keyword_groups: Dict[str, List[str]] = field(default_factory=dict)