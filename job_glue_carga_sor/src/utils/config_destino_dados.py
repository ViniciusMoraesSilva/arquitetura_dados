"""Leitura da configuracao unificada de destino do Glue."""

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


def _obter_configuracao_ambiente(
    configuracao: dict[str, Any],
    args: dict[str, str],
) -> dict[str, Any]:
    ambiente = args.get("AMBIENTE")
    if not ambiente:
        raise ValueError("Argumento AMBIENTE nao encontrado.")

    try:
        return configuracao["ambientes"][ambiente]
    except KeyError as exc:
        raise ValueError(
            f"Configuracao de destino nao encontrada para ambiente={ambiente}."
        ) from exc


def _obter_campos_tecnicos_catalogo(configuracao: dict[str, Any]) -> dict[str, Any]:
    catalogo = configuracao.get("catalogo", {})
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

    return {campo: catalogo[campo] for campo in campos_tecnicos}


def _selecionar_destino_unico(
    destinos: list[dict[str, Any]],
    filtros: dict[str, str],
    descricao: str,
) -> dict[str, Any]:
    destinos_compativeis = [
        destino
        for destino in destinos
        if all(destino.get(campo) == valor for campo, valor in filtros.items())
    ]

    if not destinos_compativeis:
        filtros_texto = ", ".join(
            f"{campo}={valor}" for campo, valor in filtros.items()
        )
        raise ValueError(
            f"Configuracao de destino {descricao} nao encontrada para {filtros_texto}."
        )

    if len(destinos_compativeis) > 1:
        raise ValueError(
            f"Mais de uma configuracao de destino {descricao} encontrada."
        )

    return destinos_compativeis[0]


def _montar_configuracao_destino(
    destino: dict[str, Any],
    configuracao: dict[str, Any],
    args: dict[str, str],
) -> dict[str, Any]:
    tipo_saida_destino = destino.get("tipo_saida_destino", "catalogo").strip().lower()
    configuracao_ambiente = _obter_configuracao_ambiente(configuracao, args)

    if tipo_saida_destino == "catalogo":
        return {
            **_obter_campos_tecnicos_catalogo(configuracao),
            **configuracao_ambiente,
            **destino,
            "tipo_saida_destino": tipo_saida_destino,
        }

    if tipo_saida_destino == "csv":
        return {
            **configuracao.get("csv", {}),
            **configuracao_ambiente,
            **destino,
            "tipo_saida_destino": tipo_saida_destino,
        }

    raise ValueError("Configuracao tipo_saida_destino invalida. Use 'catalogo' ou 'csv'.")


def obter_configuracao_destino_sor(
    metadata: dict[str, Any],
    args: dict[str, str],
) -> dict[str, Any]:
    """Seleciona o destino SOR por process_name e sor_table_name."""
    configuracao = carregar_config_destino_dados()
    destino = _selecionar_destino_unico(
        configuracao.get("destinos", []),
        {
            "modo_execucao": "sor",
            "process_name": metadata["process_name"],
            "sor_table_name": metadata["sor_table_name"],
        },
        "SOR",
    )
    return _montar_configuracao_destino(destino, configuracao, args)


def obter_configuracao_destino_sot(
    process_id: str,
    args: dict[str, str],
) -> dict[str, Any]:
    """Seleciona o destino SOT por process_id."""
    configuracao = carregar_config_destino_dados()
    destino = _selecionar_destino_unico(
        configuracao.get("destinos", []),
        {
            "modo_execucao": "sot",
            "process_id": process_id,
        },
        "SOT",
    )
    return _montar_configuracao_destino(destino, configuracao, args)


def obter_configuracao_destino(
    metadata: dict[str, Any],
    args: dict[str, str],
) -> dict[str, Any]:
    """Compatibilidade com o contrato antigo de destino SOR."""
    return obter_configuracao_destino_sor(metadata, args)
