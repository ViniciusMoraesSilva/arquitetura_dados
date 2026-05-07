"""Leitura da configuracao de origem do Glue Carga SOR."""

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


def _fonte_compativel(fonte: dict[str, Any], metadata: dict[str, Any]) -> bool:
    if fonte.get("process_name") != metadata["process_name"]:
        return False
    if fonte.get("source_database_name") != metadata["source_database_name"]:
        return False
    if fonte.get("source_table_name") != metadata["source_table_name"]:
        return False

    sor_table_name = fonte.get("sor_table_name")
    return sor_table_name in (None, metadata["sor_table_name"])


def obter_configuracao_origem(
    metadata: dict[str, Any],
    contexto: dict[str, str],
) -> dict[str, Any]:
    """Seleciona a origem configurada para process_name/origem/tabela SOR."""
    configuracao = carregar_config_origem_dados()
    fontes = configuracao.get("catalogo", {}).get("fontes", [])
    fontes_compativeis = [
        fonte for fonte in fontes if _fonte_compativel(fonte, metadata)
    ]

    if not fontes_compativeis:
        raise ValueError(
            "Configuracao de origem nao encontrada para "
            f"process_name={metadata['process_name']}, "
            f"source_database_name={metadata['source_database_name']}, "
            f"source_table_name={metadata['source_table_name']}, "
            f"sor_table_name={metadata['sor_table_name']}."
        )

    if len(fontes_compativeis) > 1:
        raise ValueError(
            "Mais de uma configuracao de origem encontrada para a metadata de ingestao."
        )

    fonte = fontes_compativeis[0]
    return {
        campo: resolver_template(campo, valor, contexto)
        for campo, valor in fonte.items()
    }
