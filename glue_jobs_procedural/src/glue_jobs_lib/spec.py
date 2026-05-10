"""Funcoes especificas do job SPEC."""

from __future__ import annotations

from typing import Any

from . import common

ARGUMENTOS_OBRIGATORIOS = [
    "JOB_NAME",
    "PROCESS_ID",
    "DATA_REF",
    "AMBIENTE",
    "DBLOCAL",
]

DEFAULT_PIPELINE_CONTROL_TABLE = "pipeline_control"


def resolver_argumentos_job() -> dict[str, str]:
    """Resolve argumentos obrigatorios do SPEC."""
    return common.resolver_argumentos_obrigatorios(ARGUMENTOS_OBRIGATORIOS)


def montar_contexto_templates(args: dict[str, str]) -> dict[str, str]:
    """Monta contexto de templates SPEC."""
    return {
        "PROCESS_ID": args["PROCESS_ID"],
        **common.montar_contexto_data(args["DATA_REF"]),
    }


def obter_ingestion_id_spec(
    dynamodb_resource: Any,
    process_id: str,
    data_ref: str,
    table_name: str = DEFAULT_PIPELINE_CONTROL_TABLE,
) -> str:
    """Busca o INGESTION_ID usado pela SPEC quando a config exigir."""
    tabela = dynamodb_resource.Table(table_name)
    resposta = tabela.get_item(
        Key={
            "PK": f"PROCESS_ID#{process_id}",
            "SK": f"DATA_REF#{data_ref}#SPEC_METADATA",
        }
    )
    item = resposta.get("Item")
    if not item or not item.get("ingestion_id"):
        raise ValueError(
            "INGESTION_ID da SPEC nao encontrado para "
            f"PROCESS_ID={process_id}, DATA_REF={data_ref}."
        )
    return str(item["ingestion_id"])


def obter_execucao(
    config_origem: dict[str, Any],
    args: dict[str, str],
    contexto: dict[str, str],
    dynamodb_resource: Any | None = None,
    pipeline_control_table: str = DEFAULT_PIPELINE_CONTROL_TABLE,
) -> dict[str, Any]:
    """Seleciona e resolve a execucao SPEC."""
    execucao = common.selecionar_unico(
        config_origem.get("execucoes", []),
        {"dominio": "spec", "process_id": args["PROCESS_ID"]},
        "execucao SPEC",
    )

    contexto_final = {**contexto}
    if common.config_usa_placeholder(execucao, "INGESTION_ID"):
        if dynamodb_resource is None:
            raise ValueError("SPEC exige dynamodb_resource para resolver INGESTION_ID.")
        contexto_final["INGESTION_ID"] = obter_ingestion_id_spec(
            dynamodb_resource,
            args["PROCESS_ID"],
            args["DATA_REF"],
            pipeline_control_table,
        )

    return common.resolver_template("execucao", execucao, contexto_final)


def montar_fontes(
    execucao: dict[str, Any],
    predicates_argumento: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    """Monta fontes SPEC."""
    return common.montar_fontes(execucao, predicates_argumento)


def obter_destino(
    config_destino: dict[str, Any],
    args: dict[str, str],
) -> dict[str, Any]:
    """Seleciona destino SPEC e valida saida unica."""
    destinos = [
        destino
        for destino in config_destino.get("destinos", [])
        if destino.get("dominio") == "spec"
        and destino.get("process_id") == args["PROCESS_ID"]
    ]
    if len(destinos) != 1:
        raise ValueError("SPEC exige exatamente um destino final configurado.")
    return common.montar_destino_final(config_destino, destinos[0], args)


def obter_colunas_tecnicas(
    execucao: dict[str, Any],
    args: dict[str, str],
) -> dict[str, str]:
    """Retorna colunas tecnicas SPEC."""
    return execucao.get("post_columns", {"process_id": args["PROCESS_ID"]})
