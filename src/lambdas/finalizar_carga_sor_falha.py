from __future__ import annotations

from src.shared.control_plane import control_plane_from_env
from src.shared.time_utils import utc_now_iso


def handler(event, context, *, control_plane=None, clock=None):
    control_plane = control_plane or control_plane_from_env()
    ingestion_id = event["ingestion_id"]
    failed_at = utc_now_iso(clock)
    control_plane.update_ingestion_metadata(
        ingestion_id,
        {"status": "FAILED", "failed_at": failed_at, "error": event.get("error", "unknown")},
    )
    return {"ingestion_id": ingestion_id, "status": "FAILED"}
