import json


def parse_jsonl_text(text: str) -> list[dict]:
    records = []

    for line in text.splitlines():
        line = line.strip()
        if line:
            records.append(json.loads(line))

    return records


def read_jsonl_from_minio(client, bucket: str, object_name: str) -> list[dict]:
    response = client.get_object(bucket, object_name)

    try:
        text = response.read().decode("utf-8")
        return parse_jsonl_text(text)
    finally:
        response.close()
        response.release_conn()