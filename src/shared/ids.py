from __future__ import annotations

import re
import uuid

from src.shared.time_utils import validate_data_referencia


def _short_uuid(id_factory=None) -> str:
    raw = id_factory() if id_factory else uuid.uuid4().hex
    return re.sub(r"[^0-9a-f]", "", raw.lower())[:12]


def new_ingestion_id(id_factory=None) -> str:
    return f"ing_{_short_uuid(id_factory)}"


def new_process_id(process_name: str, data_referencia: str, id_factory=None) -> str:
    validate_data_referencia(data_referencia)
    normalized_process = re.sub(r"[^a-z0-9]+", "_", process_name.lower()).strip("_")
    if not normalized_process:
        raise ValueError("process_name is required")
    return f"proc_{normalized_process}_{data_referencia}_{_short_uuid(id_factory)}"

