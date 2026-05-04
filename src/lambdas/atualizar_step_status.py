from __future__ import annotations

from src.shared.control_plane import control_plane_from_env
from src.shared.time_utils import utc_now_iso


def handler(event, context, *, control_plane=None, clock=None):
    control_plane = control_plane or control_plane_from_env()
    updated_at = utc_now_iso(clock)
    step_name = event["step_name"]
    process_id = event["process_id"]
    step = control_plane.put_step_status(
        process_id,
        step_name,
        {
            "status": event["status"],
            "updated_at": updated_at,
            "error": event.get("error"),
        },
    )
    if event.get("process_status"):
        control_plane.update_process_header(
            process_id,
            {"status": event["process_status"], "updated_at": updated_at},
        )
    return step
