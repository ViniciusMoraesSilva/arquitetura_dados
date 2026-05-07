"""Leitura centralizada da configuração de origem de dados."""

from __future__ import annotations

import json
from pathlib import Path

CONFIG_ORIGEM_DADOS_PATH = (
    Path(__file__).resolve().parent.parent / "config" / "config_origem_dados.json"
)


def carregar_config_origem_dados() -> dict:
    """
    Carrega o arquivo JSON de configuração de origem de dados.

    Returns:
        Dicionário com a configuração completa de origem de dados.
    """
    if not CONFIG_ORIGEM_DADOS_PATH.exists():
        raise FileNotFoundError(
            "Arquivo de configuracao de origem de dados nao encontrado: "
            f"{CONFIG_ORIGEM_DADOS_PATH}"
        )

    with CONFIG_ORIGEM_DADOS_PATH.open(encoding="utf-8") as arquivo_config:
        return json.load(arquivo_config)


def obter_configuracao_leitura_origem() -> dict:
    """
    Resolve a configuração de leitura das origens.

    Returns:
        Dicionário com os parâmetros de leitura definidos em ``catalogo``.
    """
    configuracao = carregar_config_origem_dados()
    configuracao_catalogo = configuracao.get("catalogo", {})

    if not configuracao_catalogo:
        raise ValueError(
            "Bloco catalogo nao encontrado na configuracao de origem de dados."
        )

    campos_obrigatorios = [
        "group_files",
        "group_size_bytes",
    ]
    campos_faltantes = [
        campo for campo in campos_obrigatorios if campo not in configuracao_catalogo
    ]
    if campos_faltantes:
        raise ValueError(
            "Campos obrigatorios ausentes em catalogo: "
            + ", ".join(campos_faltantes)
        )

    return {campo: configuracao_catalogo[campo] for campo in campos_obrigatorios}
