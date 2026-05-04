from __future__ import annotations

from datetime import datetime, timezone
import re


def validate_data_referencia(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"\d{6}", value):
        raise ValueError("data_referencia must use YYYYMM")
    month = int(value[4:6])
    if month < 1 or month > 12:
        raise ValueError("data_referencia month must be between 01 and 12")
    return value


def utc_now_iso(clock=None) -> str:
    if clock:
        value = clock()
        if not isinstance(value, str):
            raise ValueError("clock must return an ISO string")
        return value
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

