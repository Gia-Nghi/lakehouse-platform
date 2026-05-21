import os
from pathlib import Path
from typing import Any, Dict

import yaml


DEFAULT_CONFIG_PATH = "/opt/airflow/config/sources/chotot.yaml"


def load_chotot_config(config_path: str | None = None) -> Dict[str, Any]:
    path = config_path or os.getenv("CHOTOT_CONFIG_PATH", DEFAULT_CONFIG_PATH)

    config_file = Path(path)

    if not config_file.exists():
        raise FileNotFoundError(f"Chotot config file not found: {config_file}")

    with config_file.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)