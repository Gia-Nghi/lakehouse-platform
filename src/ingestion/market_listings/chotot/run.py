from src.ingestion.base import BaseIngestionJob
from src.ingestion.market_listings.chotot.client import ChototClient
from src.ingestion.market_listings.chotot.parser import parse_chotot_response


class ChototIngestionJob(BaseIngestionJob):
    source_name = "chotot"

    def fetch(self):
        client = ChototClient(
            base_url=self.config["base_url"],
            params=self.config.get("params", {}),
        )
        return client.fetch()

    def parse(self, raw_data):
        return parse_chotot_response(raw_data)


def run():
    job = ChototIngestionJob("/opt/airflow/config/sources/chotot.yaml")
    return job.run()


if __name__ == "__main__":
    run()