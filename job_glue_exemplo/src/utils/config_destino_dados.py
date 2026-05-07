"""Leitura centralizada da configuração de destino de dados."""

from __future__ import annotations

import json
from pathlib import Path

CONFIG_DESTINO_DADOS_PATH = (
    Path(__file__).resolve().parent.parent / "config" / "config_destino_dados.json"
)


def carregar_config_destino_dados() -> dict:
    """
    Carrega o arquivo JSON de configuração de destino de dados.

    Returns:
        Dicionário com a configuração completa de destino de dados.
    """
    if not CONFIG_DESTINO_DADOS_PATH.exists():
        raise FileNotFoundError(
            "Arquivo de configuracao de destino de dados nao encontrado: "
            f"{CONFIG_DESTINO_DADOS_PATH}"
        )

    with CONFIG_DESTINO_DADOS_PATH.open(encoding="utf-8") as arquivo_config:
        return json.load(arquivo_config)


def obter_configuracao_destino_dados(args: dict) -> dict:
    """
    Resolve a configuração final de destino de dados a partir dos argumentos do job.

    Args:
        args: Argumentos recebidos pelo job.

    Returns:
        Configuração final do modo de saída já consolidada com o ambiente.
    """
    ambiente = args.get("AMBIENTE")
    if not ambiente:
        raise ValueError("Argumento AMBIENTE nao encontrado.")

    configuracao = carregar_config_destino_dados()
    tipo_saida_destino = configuracao.get("tipo_saida_destino")
    if not tipo_saida_destino:
        raise ValueError(
            "Campo tipo_saida_destino nao encontrado na configuracao de destino de dados."
        )

    try:
        configuracao_ambiente = configuracao["ambientes"][ambiente]
    except KeyError as exc:
        raise ValueError(
            f"Configuracao de destino de dados nao encontrada para ambiente={ambiente}."
        ) from exc

    try:
        configuracao_saida = configuracao[tipo_saida_destino]
    except KeyError as exc:
        raise ValueError(
            "Configuracao tipo_saida_destino invalida. Use 'catalogo' ou 'csv'."
        ) from exc

    return {
        **configuracao_saida,
        **configuracao_ambiente,
        "tipo_saida_destino": tipo_saida_destino,
    }


def obter_configuracao_destino() -> dict:
    """
    Resolve a configuração técnica de escrita do destino de dados.

    Returns:
        Dicionário com os parâmetros técnicos definidos no bloco ``catalogo``.
    """
    configuracao = carregar_config_destino_dados()
    configuracao_catalogo = configuracao.get("catalogo")

    if not configuracao_catalogo:
        raise ValueError(
            "Bloco catalogo nao encontrado na configuracao de destino de dados."
        )

    campos_obrigatorios = [
        "group_files",
        "group_size_bytes",
        "use_glue_parquet_writer",
        "compression",
        "block_size_bytes",
    ]
    campos_faltantes = [
        campo for campo in campos_obrigatorios if campo not in configuracao_catalogo
    ]
    if campos_faltantes:
        raise ValueError(
            "Campos obrigatorios ausentes em catalogo: "
            + ", ".join(campos_faltantes)
        )

    return {
        campo: configuracao_catalogo[campo]
        for campo in campos_obrigatorios
    }
