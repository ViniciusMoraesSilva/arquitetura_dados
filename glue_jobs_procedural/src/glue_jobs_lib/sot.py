"""Funcoes especificas do job SOT."""

from __future__ import annotations

from typing import Any

from . import common

ARGUMENTOS_OBRIGATORIOS = [
    "JOB_NAME",
    "PROCESS_ID",
    "DATA_REF",
    "INGESTION_ID",
    "AMBIENTE",
    "DBLOCAL",
]


def resolver_argumentos_job() -> dict[str, str]:
    """Resolve argumentos obrigatorios do SOT."""
    return common.resolver_argumentos_obrigatorios(ARGUMENTOS_OBRIGATORIOS)


def montar_contexto_templates(args: dict[str, str]) -> dict[str, str]:
    """Monta contexto de templates SOT."""
    return {
        "PROCESS_ID": args["PROCESS_ID"],
        "INGESTION_ID": args["INGESTION_ID"],
        **common.montar_contexto_data(args["DATA_REF"]),
    }


def obter_execucao(
    config_origem: dict[str, Any],
    args: dict[str, str],
    contexto: dict[str, str],
) -> dict[str, Any]:
    """Seleciona e resolve a execucao SOT."""
    execucao = common.selecionar_unico(
        config_origem.get("execucoes", []),
        {"dominio": "sot", "process_id": args["PROCESS_ID"]},
        "execucao SOT",
    )
    return common.resolver_template("execucao", execucao, contexto)


def montar_fontes(
    execucao: dict[str, Any],
    predicates_argumento: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    """Monta fontes SOT."""
    return common.montar_fontes(execucao, predicates_argumento)


def obter_destino(
    config_destino: dict[str, Any],
    args: dict[str, str],
) -> dict[str, Any]:
    """Seleciona destino SOT."""
    destino = common.selecionar_unico(
        config_destino.get("destinos", []),
        {"dominio": "sot", "process_id": args["PROCESS_ID"]},
        "destino SOT",
    )
    return common.montar_destino_final(config_destino, destino, args)


def obter_colunas_tecnicas(
    execucao: dict[str, Any],
    args: dict[str, str],
) -> dict[str, str]:
    """Retorna colunas tecnicas SOT."""
    return execucao.get("post_columns", {"process_id": args["PROCESS_ID"]})
