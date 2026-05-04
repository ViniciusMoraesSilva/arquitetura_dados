from __future__ import annotations

from src.shared.control_plane import control_plane_from_env
from src.shared.time_utils import utc_now_iso


def handler(event, context, *, control_plane=None, clock=None):
    control_plane = control_plane or control_plane_from_env()
    ingestion_id = event["ingestion_id"]
    loaded_at = utc_now_iso(clock)
    metadata = control_plane.update_ingestion_metadata(
        ingestion_id,
        {"status": "SOR_LOADED", "loaded_at": loaded_at},
    )
    history = {
        "ingestion_id": ingestion_id,
        "table_name": metadata["table_name"],
        "data_referencia": metadata["data_referencia"],
        "status": "SOR_LOADED",
        "loaded_at": loaded_at,
    }
    control_plane.put_ingestion_history(
        metadata["table_name"],
        metadata["data_referencia"],
        ingestion_id,
        history,
    )
    control_plane.update_current_sor(
        metadata["table_name"],
        metadata["data_referencia"],
        history,
    )
    return {"ingestion_id": ingestion_id, "status": "SOR_LOADED"}
