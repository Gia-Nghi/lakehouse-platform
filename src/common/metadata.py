from datetime import datetime, timezone
from typing import Any, Optional


DEFAULT_SCHEMA_VERSION = "v2"


def now_utc_iso() -> str:
    """
    Trả về thời gian hiện tại theo UTC dạng ISO string.
    Dùng cho metadata để biết record được hệ thống xử lý lúc nào.
    """
    return datetime.now(timezone.utc).isoformat()


def build_ingestion_metadata(
    *,
    source: str,
    category: str,
    entity: str,
    ingestion_type: str,
    pipeline_name: str,
    schema_version: str = DEFAULT_SCHEMA_VERSION,
    run_id: Optional[str] = None,
    migrated_from: Optional[str] = None,
    original_ingestion_type: Optional[str] = None,
    extra: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """
    Build phần metadata chuẩn dùng chung cho các source ingestion.

    Output dạng:

    {
        "source": "...",
        "category": "...",
        "entity": "...",
        "metadata": {
            "schema_version": "v2",
            "ingestion_type": "...",
            "pipeline_name": "...",
            "ingested_at_utc": "..."
        }
    }
    """
    metadata: dict[str, Any] = {
        "schema_version": schema_version,
        "ingestion_type": ingestion_type,
        "pipeline_name": pipeline_name,
        "ingested_at_utc": now_utc_iso(),
    }

    if run_id:
        metadata["run_id"] = run_id

    if migrated_from:
        metadata["migrated_from"] = migrated_from

    if original_ingestion_type:
        metadata["original_ingestion_type"] = original_ingestion_type

    if extra:
        metadata.update(extra)

    return {
        "source": source,
        "category": category,
        "entity": entity,
        "metadata": metadata,
    }