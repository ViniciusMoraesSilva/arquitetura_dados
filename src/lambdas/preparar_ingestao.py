from __future__ import annotations

from src.shared.ids import new_ingestion_id
from src.shared.control_plane import control_plane_from_env
from src.shared.time_utils import utc_now_iso, validate_data_referencia


def handler(event, context, *, control_plane=None, id_factory=None, clock=None):
    control_plane = control_plane or control_plane_from_env()
    if "process_name" in event:
        raise ValueError("ingestion is generic and must not receive process_name")

    table_name = event["table_name"]
    data_referencia = validate_data_referencia(event["data_referencia"])
    source_type = event["source_type"]
    if source_type not in {"glue_catalog", "csv_s3"}:
        raise ValueError("source_type must be glue_catalog or csv_s3")

    ingestion_id = new_ingestion_id(id_factory)
    created_at = utc_now_iso(clock)
    metadata = {
        "ingestion_id": ingestion_id,
        "table_name": table_name,
        "data_referencia": data_referencia,
        "source_type": source_type,
        "status": "PROCESSING",
        "created_at": created_at,
    }

    if source_type == "glue_catalog":
        metadata.update(
            {
                "database_name": event["database_name"],
                "partition_values": event.get("partition_values", {}),
            }
        )
    else:
        metadata.update(
            {
                "s3_path": event["s3_path"],
                "csv_options": event.get("csv_options", {}),
            }
        )

    control_plane.create_ingestion_metadata(ingestion_id, metadata)
    return {
        "ingestion_id": ingestion_id,
        "status": "PROCESSING",
        "glue_arguments": {"--INGESTION_ID": ingestion_id},
    }
