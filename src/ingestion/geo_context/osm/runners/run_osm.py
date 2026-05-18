from src.ingestion.base import BaseIngestionJob
from src.ingestion.geo_context.osm.client import OSMClient
from src.ingestion.geo_context.osm.parser import parse_osm_gdf


class OSMIngestionJob(BaseIngestionJob):
    source_name = "osm"

    def fetch(self):
        client = OSMClient()

        all_frames = []

        for place in self.config["places"]:
            gdf = client.fetch_features(
                place=place,
                tags=self.config["tags"],
            )
            all_frames.append(gdf)

        return all_frames

    def parse(self, raw_data):
        records = []

        for gdf in raw_data:
            records.extend(parse_osm_gdf(gdf))

        return records


def run():
    job = OSMIngestionJob("/opt/airflow/config/sources/osm.yaml")
    return job.run()


if __name__ == "__main__":
    run()