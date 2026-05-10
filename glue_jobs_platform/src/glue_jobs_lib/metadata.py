"""Leitura de metadados em DynamoDB."""

from __future__ import annotations

from typing import Any

SOR_STATUS = "PROCESSING_SOR"
DEFAULT_PIPELINE_CONTROL_TABLE = "pipeline_control"


def get_sor_metadata(
    dynamodb_resource: Any,
    ingestion_id: str,
    table_name: str = DEFAULT_PIPELINE_CONTROL_TABLE,
) -> dict[str, Any]:
    """Busca e valida metadata SOR por INGESTION_ID."""
    item = dynamodb_resource.Table(table_name).get_item(
        Key={"PK": f"INGESTION_ID#{ingestion_id}", "SK": "METADATA"}
    ).get("Item")
    if not item:
        raise ValueError(f"Metadata SOR nao encontrada para INGESTION_ID={ingestion_id}.")
    if str(item.get("ingestion_id")) != str(ingestion_id):
        raise ValueError("Metadata SOR retornada nao corresponde ao INGESTION_ID.")
    if item.get("status") != SOR_STATUS:
        raise ValueError(
            f"Ingestao SOR nao esta no status esperado: {SOR_STATUS}."
        )
    return item


def get_spec_ingestion_id(
    dynamodb_resource: Any,
    process_id: str,
    data_ref: str,
    table_name: str = DEFAULT_PIPELINE_CONTROL_TABLE,
) -> str:
    """Resolve INGESTION_ID usado por SPEC via PROCESS_ID + DATA_REF."""
    item = dynamodb_resource.Table(table_name).get_item(
        Key={
            "PK": f"PROCESS_ID#{process_id}",
            "SK": f"DATA_REF#{data_ref}#SPEC_METADATA",
        }
    ).get("Item")
    if not item or not item.get("ingestion_id"):
        raise ValueError(
            "INGESTION_ID da SPEC nao encontrado para "
            f"PROCESS_ID={process_id}, DATA_REF={data_ref}."
        )
    return str(item["ingestion_id"])
