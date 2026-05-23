import json
import tempfile
from datetime import datetime, timezone

import boto3
import requests
import yaml


CONFIG_PATH = "/opt/airflow/config/sources/osm.yaml"


OVERPASS_URL = "https://overpass-api.de/api/interpreter"


def load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def build_overpass_query():
    return """
    [out:json][timeout:180];

    area["name"="Ho Chi Minh City"]->.searchArea;

    (
      node["amenity"](area.searchArea);
      way["amenity"](area.searchArea);

      way["highway"](area.searchArea);

      node["public_transport"](area.searchArea);
      way["public_transport"](area.searchArea);

      way["railway"](area.searchArea);

      relation["boundary"="administrative"](area.searchArea);
    );

    out center tags;
    """


def fetch_osm_data():
    query = build_overpass_query()

    response = requests.post(
        OVERPASS_URL,
        data=query,
        timeout=180,
    )

    response.raise_for_status()

    return response.json()


def normalize_record(element):
    return {
        "osm_id": element.get("id"),
        "osm_type": element.get("type"),
        "lat": element.get("lat"),
        "lon": element.get("lon"),
        "tags": element.get("tags", {}),
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "source": "osm",
        "region": "ho_chi_minh_city",
    }


def upload_to_minio(local_file, bucket, object_key):
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
    )

    s3.upload_file(local_file, bucket, object_key)


def main():
    config = load_config()

    bucket = config["storage"]["bucket"]
    bronze_prefix = config["storage"]["bronze_prefix"]

    now = datetime.now(timezone.utc)

    date_partition = now.strftime("%Y_%m_%d")
    file_date = now.strftime("%Y%m%d")

    filename = f"osm_hcm_{file_date}_0000.jsonl"

    object_key = (
        f"{bronze_prefix}"
        f"date={date_partition}/"
        f"{filename}"
    )

    osm_data = fetch_osm_data()

    elements = osm_data.get("elements", [])

    with tempfile.NamedTemporaryFile(mode="w", delete=False) as tmp:
        for element in elements:
            record = normalize_record(element)

            tmp.write(
                json.dumps(record, ensure_ascii=False) + "\n"
            )

        temp_path = tmp.name

    upload_to_minio(
        local_file=temp_path,
        bucket=bucket,
        object_key=object_key,
    )

    print(f"Uploaded to s3://{bucket}/{object_key}")


if __name__ == "__main__":
    main()