import time

from src.ingestion.geo_context.osm.config import OVERPASS_SLEEP_BETWEEN_ENTITY_SECONDS
from src.ingestion.geo_context.osm.registry import OSM_ENTITY_REGISTRY
from src.ingestion.geo_context.osm.runners._base import (
    collect_by_grid,
    upload_layer_jsonl,
    upload_metadata,
)


def run() -> dict:
    summary = {}

    for entity_name, spec in OSM_ENTITY_REGISTRY.items():
        print(f"[OSM] Start collecting entity={entity_name}")

        raw_elements = collect_by_grid(
            entity_name=entity_name,
            query_builder=spec["query_builder"],
        )

        parsed_payload = spec["parser"](raw_elements)

        object_name = upload_layer_jsonl(
            entity_type=spec["entity_type"],
            parsed_payload=parsed_payload,
            batch_id=0,
        )

        summary[entity_name] = {
            "raw_count": len(raw_elements),
            "parsed_count": len(parsed_payload),
            "object_name": object_name,
        }

        print(f"[OSM] Finished entity={entity_name}, parsed={len(parsed_payload)}")

        time.sleep(OVERPASS_SLEEP_BETWEEN_ENTITY_SECONDS)

    metadata_object = upload_metadata(summary)

    return {
        "status": "success",
        "summary": summary,
        "metadata_object": metadata_object,
    }


if __name__ == "__main__":
    run()