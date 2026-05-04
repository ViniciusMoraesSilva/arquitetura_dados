from __future__ import annotations

from src.shared.control_plane import control_plane_from_env
from src.shared.ids import new_process_id
from src.shared.time_utils import utc_now_iso, validate_data_referencia


def handler(event, context, *, control_plane=None, id_factory=None, clock=None):
    control_plane = control_plane or control_plane_from_env()
    process_name = event["process_name"]
    data_referencia = validate_data_referencia(event["data_referencia"])
    created_at = utc_now_iso(clock)
    process_id = new_process_id(process_name, data_referencia, id_factory)
    required_tables = control_plane.list_full_config_tables(process_name)
    missing_tables = [
        table_name
        for table_name in required_tables
        if control_plane.get_current_sor(table_name, data_referencia) is None
    ]

    status = "FAILED_INPUT_MISSING" if missing_tables else "READY"
    header = {
        "process_id": process_id,
        "process_name": process_name,
        "data_referencia": data_referencia,
        "status": status,
        "created_at": created_at,
    }
    if missing_tables:
        header["missing_tables"] = missing_tables
    control_plane.put_process_header(process_id, header)
    control_plane.put_process_list(process_name, data_referencia, process_id, header)

    if missing_tables:
        return {"process_id": process_id, "status": status, "missing_tables": missing_tables}

    for table_name in required_tables:
        current = control_plane.get_current_sor(table_name, data_referencia)
        control_plane.create_input_lock(
            process_id,
            table_name,
            {
                "ingestion_id": current["ingestion_id"],
                "data_referencia": data_referencia,
                "locked_at": created_at,
            },
        )

    return {"process_id": process_id, "status": status}
