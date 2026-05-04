from __future__ import annotations


def build_catalog_pushdown_predicate(metadata: dict) -> str:
    partitions = metadata.get("partition_values", {})
    return " and ".join(f"{name}='{value}'" for name, value in sorted(partitions.items()))


def resolve_sor_load_plan(ingestion_id: str, control_plane) -> dict:
    metadata = control_plane.get_ingestion_metadata(ingestion_id)
    source_type = metadata["source_type"]
    if source_type == "glue_catalog":
        source = {
            "database_name": metadata["database_name"],
            "table_name": metadata["table_name"],
            "pushdown_predicate": build_catalog_pushdown_predicate(metadata),
        }
    elif source_type == "csv_s3":
        source = {
            "s3_path": metadata["s3_path"],
            "csv_options": metadata.get("csv_options", {}),
        }
    else:
        raise ValueError(f"unsupported source_type: {source_type}")
    return {
        "arguments": {"INGESTION_ID": ingestion_id},
        "metadata": metadata,
        "read_mode": source_type,
        "source": source,
        "sor_write": {
            "table_name": metadata["table_name"],
            "partition_keys": ["ingestion_id"],
            "extra_columns": {"ingestion_id": ingestion_id},
        },
    }

