from dataclasses import dataclass
from typing import List


@dataclass
class GoogleTrendsConfig:
    keywords: List[str]
    geo: str = "VN"
    timeframe: str = "now 7-d"