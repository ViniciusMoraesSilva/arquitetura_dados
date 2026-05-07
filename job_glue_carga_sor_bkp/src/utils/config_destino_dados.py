"""Leitura da configuracao de destino do Glue Carga SOR."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

CONFIG_DESTINO_DADOS_PATH = (
    Path(__file__).resolve().parent.parent / "config" / "config_destino_dados.json"
)


def carregar_config_destino_dados() -> dict[str, Any]:
    """Carrega o arquivo JSON de configuracao de destino."""
    if not CONFIG_DESTINO_DADOS_PATH.exists():
        raise FileNotFoundError(
            "Arquivo de configuracao de destino de dados nao encontrado: "
            f"{CONFIG_DESTINO_DADOS_PATH}"
        )

    with CONFIG_DESTINO_DADOS_PATH.open(encoding="utf-8") as arquivo_config:
        return json.load(arquivo_config)


def obter_configuracao_destino(metadata: dict[str, Any], args: dict[str, str]) -> dict[str, Any]:
    """Seleciona o destino configurado por process_name e sor_table_name."""
    ambiente = args.get("AMBIENTE")
    if not ambiente:
        raise ValueError("Argumento AMBIENTE nao encontrado.")

    configuracao = carregar_config_destino_dados()
    tipo_saida_destino = configuracao.get("tipo_saida_destino", "").strip().lower()
    if tipo_saida_destino != "catalogo":
        raise ValueError("Glue Carga SOR suporta apenas tipo_saida_destino='catalogo'.")

    try:
        configuracao_ambiente = configuracao["ambientes"][ambiente]
    except KeyError as exc:
        raise ValueError(
            f"Configuracao de destino nao encontrada para ambiente={ambiente}."
        ) from exc

    catalogo = configuracao.get("catalogo", {})
    destinos = catalogo.get("destinos", [])
    destinos_compativeis = [
        destino
        for destino in destinos
        if destino.get("process_name") == metadata["process_name"]
        and destino.get("sor_table_name") == metadata["sor_table_name"]
    ]

    if not destinos_compativeis:
        raise ValueError(
            "Configuracao de destino nao encontrada para "
            f"process_name={metadata['process_name']}, "
            f"sor_table_name={metadata['sor_table_name']}."
        )

    if len(destinos_compativeis) > 1:
        raise ValueError(
            "Mais de uma configuracao de destino encontrada para a metadata de ingestao."
        )

    campos_tecnicos = [
        "group_files",
        "group_size_bytes",
        "use_glue_parquet_writer",
        "compression",
        "block_size_bytes",
    ]
    faltantes = [campo for campo in campos_tecnicos if campo not in catalogo]
    if faltantes:
        raise ValueError(
            "Campos tecnicos obrigatorios ausentes em catalogo: "
            + ", ".join(faltantes)
        )

    return {
        **{campo: catalogo[campo] for campo in campos_tecnicos},
        **destinos_compativeis[0],
        **configuracao_ambiente,
        "tipo_saida_destino": tipo_saida_destino,
    }
