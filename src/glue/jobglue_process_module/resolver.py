from __future__ import annotations


def build_ingestion_predicate(ingestion_id: str) -> str:
    return f"ingestion_id='{ingestion_id}'"


def resolve_locked_sources(process_id: str, table_names: list[str], control_plane) -> dict:
    sources = {}
    for table_name in table_names:
        lock = control_plane.require_input_lock(process_id, table_name)
        ingestion_id = lock["ingestion_id"]
        sources[table_name] = {
            "table_name": table_name,
            "ingestion_id": ingestion_id,
            "predicate": build_ingestion_predicate(ingestion_id),
        }
    return sources
