from src.ingestion.base import BaseIngestionJob
from src.ingestion.user_interest.google_trends.collector import GoogleTrendsCollector


class GoogleTrendsIngestionJob(BaseIngestionJob):
    source_name = "google_trends"

    def fetch(self):
        collector = GoogleTrendsCollector(
            geo=self.config.get("geo", "VN"),
            timeframe=self.config.get("timeframe", "now 7-d"),
        )

        return collector.collect(self.config["keywords"])

    def parse(self, raw_data):
        return raw_data


def run():
    job = GoogleTrendsIngestionJob("/opt/airflow/config/sources/google_trends.yaml")
    return job.run()


if __name__ == "__main__":
    run()