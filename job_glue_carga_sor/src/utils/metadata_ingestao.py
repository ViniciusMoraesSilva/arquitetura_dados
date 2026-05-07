"""Leitura e validacao da metadata de ingestao SOR."""

from __future__ import annotations

from typing import Any

STATUS_ESPERADO = "PROCESSING_SOR"


def validar_metadata_ingestao(metadata: dict[str, Any], ingestion_id: str) -> dict[str, Any]:
    """Valida o item INGESTION_ID/METADATA usado pelo Glue Carga SOR."""
    campos_obrigatorios = [
        "ingestion_id",
        "process_name",
        "source_database_name",
        "source_table_name",
        "sor_database_name",
        "sor_table_name",
        "partitions",
        "status",
    ]
    faltantes = [campo for campo in campos_obrigatorios if campo not in metadata]
    if faltantes:
        raise ValueError(
            "Campos obrigatorios ausentes na metadata de ingestao: "
            + ", ".join(faltantes)
        )

    if str(metadata["ingestion_id"]) != str(ingestion_id):
        raise ValueError(
            "Metadata retornada nao corresponde ao INGESTION_ID solicitado: "
            f"esperado={ingestion_id}, recebido={metadata['ingestion_id']}"
        )

    if metadata["status"] != STATUS_ESPERADO:
        raise ValueError(
            "Ingestao nao esta no status esperado para carga SOR: "
            f"esperado={STATUS_ESPERADO}, recebido={metadata['status']}"
        )

    if not isinstance(metadata["partitions"], dict) or not metadata["partitions"]:
        raise ValueError("Metadata de ingestao deve conter partitions como dict nao vazio.")

    return metadata


def obter_metadata_ingestao(
    dynamodb_resource: Any,
    table_name: str,
    ingestion_id: str,
) -> dict[str, Any]:
    """Busca e valida a metadata de ingestao no DynamoDB pipeline_control."""
    tabela = dynamodb_resource.Table(table_name)
    resposta = tabela.get_item(
        Key={
            "PK": f"INGESTION_ID#{ingestion_id}",
            "SK": "METADATA",
        }
    )
    item = resposta.get("Item")
    if not item:
        raise ValueError(
            "Metadata de ingestao nao encontrada no DynamoDB para "
            f"INGESTION_ID={ingestion_id}"
        )

    return validar_metadata_ingestao(item, ingestion_id)
