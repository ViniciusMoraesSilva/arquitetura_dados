"""Leitura da configuracao unificada de origem do Glue."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .templates import resolver_template

CONFIG_ORIGEM_DADOS_PATH = (
    Path(__file__).resolve().parent.parent / "config" / "config_origem_dados.json"
)


def carregar_config_origem_dados() -> dict[str, Any]:
    """Carrega o arquivo JSON de configuracao de origem."""
    if not CONFIG_ORIGEM_DADOS_PATH.exists():
        raise FileNotFoundError(
            "Arquivo de configuracao de origem de dados nao encontrado: "
            f"{CONFIG_ORIGEM_DADOS_PATH}"
        )

    with CONFIG_ORIGEM_DADOS_PATH.open(encoding="utf-8") as arquivo_config:
        return json.load(arquivo_config)


def obter_configuracao_leitura_origem() -> dict[str, Any]:
    """Retorna parametros tecnicos de leitura definidos no bloco catalogo."""
    configuracao_catalogo = carregar_config_origem_dados().get("catalogo", {})
    campos_obrigatorios = ["group_files", "group_size_bytes"]
    faltantes = [
        campo for campo in campos_obrigatorios if campo not in configuracao_catalogo
    ]
    if faltantes:
        raise ValueError(
            "Campos obrigatorios ausentes em catalogo: " + ", ".join(faltantes)
        )

    return {campo: configuracao_catalogo[campo] for campo in campos_obrigatorios}


def resolver_templates_configuracao(
    configuracao: Any,
    contexto: dict[str, str],
) -> Any:
    """Resolve templates em uma estrutura JSON composta por dict/list/str."""
    if isinstance(configuracao, dict):
        return {
            campo: resolver_templates_configuracao(valor, contexto)
            for campo, valor in configuracao.items()
        }

    if isinstance(configuracao, list):
        return [
            resolver_templates_configuracao(valor, contexto)
            for valor in configuracao
        ]

    return resolver_template("configuracao", configuracao, contexto)


def _execucao_sor_compativel(
    execucao: dict[str, Any],
    metadata: dict[str, Any],
) -> bool:
    if execucao.get("modo_execucao") != "sor":
        return False
    if execucao.get("process_name") != metadata["process_name"]:
        return False
    if execucao.get("source_database_name") != metadata["source_database_name"]:
        return False
    if execucao.get("source_table_name") != metadata["source_table_name"]:
        return False

    sor_table_name = execucao.get("sor_table_name")
    return sor_table_name in (None, metadata["sor_table_name"])


def obter_configuracao_execucao_sor(
    metadata: dict[str, Any],
    contexto: dict[str, str],
) -> dict[str, Any]:
    """Seleciona a execucao SOR configurada para a metadata de ingestao."""
    configuracao = carregar_config_origem_dados()
    execucoes = configuracao.get("execucoes", [])
    execucoes_compativeis = [
        execucao for execucao in execucoes if _execucao_sor_compativel(execucao, metadata)
    ]

    if not execucoes_compativeis:
        raise ValueError(
            "Configuracao de execucao SOR nao encontrada para "
            f"process_name={metadata['process_name']}, "
            f"source_database_name={metadata['source_database_name']}, "
            f"source_table_name={metadata['source_table_name']}, "
            f"sor_table_name={metadata['sor_table_name']}."
        )

    if len(execucoes_compativeis) > 1:
        raise ValueError(
            "Mais de uma configuracao de execucao SOR encontrada para a metadata."
        )

    return resolver_templates_configuracao(execucoes_compativeis[0], contexto)


def obter_configuracao_execucao_sot(
    process_id: str,
    contexto: dict[str, str],
) -> dict[str, Any]:
    """Seleciona a execucao SOT configurada por process_id."""
    configuracao = carregar_config_origem_dados()
    execucoes = configuracao.get("execucoes", [])
    execucoes_compativeis = [
        execucao
        for execucao in execucoes
        if execucao.get("modo_execucao") == "sot"
        and execucao.get("process_id") == process_id
    ]

    if not execucoes_compativeis:
        raise ValueError(
            f"Configuracao de execucao SOT nao encontrada para process_id={process_id}."
        )

    if len(execucoes_compativeis) > 1:
        raise ValueError(
            f"Mais de uma configuracao de execucao SOT encontrada para process_id={process_id}."
        )

    return resolver_templates_configuracao(execucoes_compativeis[0], contexto)


def obter_configuracao_origem(
    metadata: dict[str, Any],
    contexto: dict[str, str],
) -> dict[str, Any]:
    """
    Compatibilidade com o contrato antigo: retorna a primeira fonte SOR.

    Novos fluxos devem usar ``obter_configuracao_execucao_sor`` para obter a
    execucao completa, incluindo SQL e lista de fontes.
    """
    execucao = obter_configuracao_execucao_sor(metadata, contexto)
    fontes = execucao.get("fontes", [])
    if not fontes:
        raise ValueError("Execucao SOR nao possui fontes configuradas.")

    if len(fontes) > 1:
        raise ValueError(
            "Mais de uma fonte encontrada na execucao SOR; use obter_configuracao_execucao_sor."
        )

    return fontes[0]
