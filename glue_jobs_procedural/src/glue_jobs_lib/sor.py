"""Funcoes especificas do job SOR."""

from __future__ import annotations

from typing import Any

from . import common

ARGUMENTOS_OBRIGATORIOS = [
    "JOB_NAME",
    "INGESTION_ID",
    "AMBIENTE",
    "DBLOCAL",
]
DEFAULT_PIPELINE_CONTROL_TABLE = "pipeline_control"
STATUS_ESPERADO = "PROCESSING_SOR"


def resolver_argumentos_job() -> dict[str, str]:
    """Resolve argumentos obrigatorios do SOR."""
    return common.resolver_argumentos_obrigatorios(ARGUMENTOS_OBRIGATORIOS)


def obter_metadata_ingestao(
    dynamodb_resource: Any,
    ingestion_id: str,
    table_name: str = DEFAULT_PIPELINE_CONTROL_TABLE,
) -> dict[str, Any]:
    """Busca e valida metadata SOR por INGESTION_ID."""
    tabela = dynamodb_resource.Table(table_name)
    resposta = tabela.get_item(
        Key={
            "PK": f"INGESTION_ID#{ingestion_id}",
            "SK": "METADATA",
        }
    )
    metadata = resposta.get("Item")
    if not metadata:
        raise ValueError(f"Metadata de ingestao nao encontrada para INGESTION_ID={ingestion_id}.")
    if str(metadata.get("ingestion_id")) != str(ingestion_id):
        raise ValueError("Metadata retornada nao corresponde ao INGESTION_ID solicitado.")
    if metadata.get("status") != STATUS_ESPERADO:
        raise ValueError(
            "Ingestao nao esta no status esperado para carga SOR: "
            f"esperado={STATUS_ESPERADO}, recebido={metadata.get('status')}"
        )
    return metadata


def montar_contexto_templates(metadata: dict[str, Any]) -> dict[str, str]:
    """Monta contexto de templates SOR."""
    contexto = {
        "INGESTION_ID": str(metadata["ingestion_id"]),
        "PROCESS_NAME": str(metadata["process_name"]),
        "SOURCE_DATABASE_NAME": str(metadata["source_database_name"]),
        "SOURCE_TABLE_NAME": str(metadata["source_table_name"]),
        "SOR_DATABASE_NAME": str(metadata["sor_database_name"]),
        "SOR_TABLE_NAME": str(metadata["sor_table_name"]),
    }
    for chave, valor in metadata.get("partitions", {}).items():
        nome_placeholder = f"PARTITION_{common.normalizar_nome_placeholder(str(chave))}"
        contexto[nome_placeholder] = str(valor)
    return contexto


def obter_execucao(
    config_origem: dict[str, Any],
    metadata: dict[str, Any],
    contexto: dict[str, str],
) -> dict[str, Any]:
    """Seleciona e resolve execucao SOR."""
    execucao = common.selecionar_unico(
        config_origem.get("execucoes", []),
        {
            "dominio": "sor",
            "process_name": metadata["process_name"],
            "sor_table_name": metadata["sor_table_name"],
        },
        "execucao SOR",
    )
    return common.resolver_template("execucao", execucao, contexto)


def montar_fontes(
    execucao: dict[str, Any],
    predicates_argumento: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    """Monta fontes SOR."""
    return common.montar_fontes(execucao, predicates_argumento)


def obter_destino(
    config_destino: dict[str, Any],
    metadata: dict[str, Any],
    args: dict[str, str],
) -> dict[str, Any]:
    """Seleciona destino SOR."""
    destino = common.selecionar_unico(
        config_destino.get("destinos", []),
        {
            "dominio": "sor",
            "process_name": metadata["process_name"],
            "sor_table_name": metadata["sor_table_name"],
        },
        "destino SOR",
    )
    return common.montar_destino_final(config_destino, destino, args)


def obter_colunas_tecnicas(
    execucao: dict[str, Any],
    args: dict[str, str],
) -> dict[str, str]:
    """Retorna colunas tecnicas SOR."""
    return execucao.get("post_columns", {"ingestion_id": args["INGESTION_ID"]})
