import random
import time
from datetime import datetime, timedelta
from typing import List, Tuple

import pandas as pd
from pytrends.request import TrendReq

from src.common.retry import retry


class GoogleTrendsCollector:
    def __init__(
        self,
        geo: str = "VN",
        timeframe: str | None = None,
        target_region: str = "Vietnam",
        months_back: int = 12,
        window_days: int = 30,
        load_type: str = "initial",
    ):
        self.geo = geo
        self.timeframe = timeframe
        self.target_region = target_region
        self.months_back = months_back
        self.window_days = window_days
        self.load_type = load_type

        self.client = TrendReq(
            hl="vi-VN",
            tz=420,
            timeout=(10, 25),
        )

    def generate_date_windows(self) -> List[Tuple[str, str]]:
        end_date = datetime.today().date()
        start_date = end_date - timedelta(
            days=self.months_back * 30
        )

        windows = []
        current = start_date

        while current < end_date:
            window_end = min(
                current + timedelta(days=self.window_days),
                end_date,
            )

            windows.append(
                (
                    current.strftime("%Y-%m-%d"),
                    window_end.strftime("%Y-%m-%d"),
                )
            )

            current = window_end

        return windows

    @retry(
        max_attempts=5,
        delay_seconds=60,
        backoff_factor=2,
        max_delay_seconds=600,
    )
    def collect(
        self,
        keyword_group: str,
        keywords: List[str],
    ) -> List[dict]:

        all_frames = []

        windows = self.generate_date_windows()

        for start_date, end_date in windows:
            timeframe = f"{start_date} {end_date}"

            sleep_time = random.randint(10, 30)
            time.sleep(sleep_time)

            self.client.build_payload(
                kw_list=keywords,
                geo=self.geo,
                timeframe=timeframe,
            )

            df = self.client.interest_over_time()

            if not df.empty:
                df["timeframe_start"] = start_date
                df["timeframe_end"] = end_date
                all_frames.append(df)

        if not all_frames:
            return []

        df = pd.concat(all_frames)

        df = (
            df[~df.index.duplicated(keep="last")]
            .sort_index()
        )

        df = df.reset_index()

        collected_at_ms = int(time.time() * 1000)

        records = []

        for row in df.to_dict(orient="records"):
            record = {
                "load_type": self.load_type,
                "date": row["date"].date().isoformat(),
                "keyword_group": keyword_group,
                "values": {
                    keyword: int(row.get(keyword, 0))
                    for keyword in keywords
                },
                "isPartial": bool(row.get("isPartial", False)),
                "timeframe_start": row.get("timeframe_start"),
                "timeframe_end": row.get("timeframe_end"),
                "collected_at_ms": collected_at_ms,
            }

            records.append(record)

        return records