from pytrends.request import TrendReq

from src.common.retry import retry


class GoogleTrendsCollector:
    def __init__(self, geo: str, timeframe: str):
        self.geo = geo
        self.timeframe = timeframe
        self.client = TrendReq(hl="vi-VN", tz=420)

    @retry(max_attempts=3, delay_seconds=5)
    def collect(self, keywords):
        self.client.build_payload(
            kw_list=keywords,
            geo=self.geo,
            timeframe=self.timeframe,
        )

        df = self.client.interest_over_time()

        if df.empty:
            return []

        df = df.reset_index()

        records = []
        for row in df.to_dict(orient="records"):
            for keyword in keywords:
                if keyword in row:
                    records.append(
                        {
                            "keyword": keyword,
                            "date": str(row["date"]),
                            "interest": int(row[keyword]),
                            "geo": self.geo,
                            "timeframe": self.timeframe,
                        }
                    )

        return records