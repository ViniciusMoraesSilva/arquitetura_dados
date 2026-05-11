"""Funcoes especificas do job SPEC."""

from __future__ import annotations

from typing import Any

from . import common

ARGUMENTOS_OBRIGATORIOS = [
    "JOB_NAME",
    "PROCESS_ID",
    "AMBIENTE",
    "DBLOCAL",
]


def resolver_argumentos_job() -> dict[str, str]:
    """Resolve argumentos obrigatorios do SPEC."""
    return common.resolver_argumentos_obrigatorios(ARGUMENTOS_OBRIGATORIOS)


def montar_contexto_templates(
    args: dict[str, str],
    dynamodb_resource: Any,
    pipeline_control_table: str = common.DEFAULT_PIPELINE_CONTROL_TABLE,
) -> dict[str, str]:
    """Monta contexto de templates SPEC."""
    metadata = common.obter_metadata_processo(
        dynamodb_resource,
        args["PROCESS_ID"],
        pipeline_control_table,
    )
    return common.montar_contexto_processo(metadata)


def obter_execucao(
    config_origem: dict[str, Any],
    args: dict[str, str],
    contexto: dict[str, str],
    dynamodb_resource: Any | None = None,
    pipeline_control_table: str = common.DEFAULT_PIPELINE_CONTROL_TABLE,
) -> dict[str, Any]:
    """Seleciona e resolve a execucao SPEC."""
    execucao = common.selecionar_unico(
        config_origem.get("execucoes", []),
        {"dominio": "spec", "process_name": contexto["PROCESS_NAME"]},
        "execucao SPEC",
    )
    input_locks = {}
    if common.config_usa_placeholder(execucao, "INGESTION_ID"):
        if dynamodb_resource is None:
            raise ValueError("SPEC exige dynamodb_resource para resolver INPUT_LOCK.")
        input_locks = common.obter_input_locks_processo(
            dynamodb_resource,
            args["PROCESS_ID"],
            pipeline_control_table,
        )
    return common.resolver_execucao_com_input_locks(execucao, contexto, input_locks)


def montar_fontes(
    execucao: dict[str, Any],
    predicates_argumento: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    """Monta fontes SPEC."""
    return common.montar_fontes(execucao, predicates_argumento)


def obter_destino(
    config_destino: dict[str, Any],
    args: dict[str, str],
    contexto: dict[str, str],
) -> dict[str, Any]:
    """Seleciona destino SPEC e valida saida unica."""
    destinos = [
        destino
        for destino in config_destino.get("destinos", [])
        if destino.get("dominio") == "spec"
        and destino.get("process_name") == contexto["PROCESS_NAME"]
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
