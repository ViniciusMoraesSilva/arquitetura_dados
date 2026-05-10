"""Job Glue SOT procedural."""

from __future__ import annotations

import logging
from pathlib import Path

from glue_jobs_lib import common, sot

MSG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
logging.basicConfig(level=logging.INFO, format=MSG_FORMAT, datefmt=DATETIME_FORMAT)
logger = logging.getLogger(__name__)

BASE_PATH = Path(__file__).resolve().parent
CONFIG_ORIGEM_PATH = BASE_PATH / "config" / "config_origem_dados.json"
CONFIG_DESTINO_PATH = BASE_PATH / "config" / "config_destino_dados.json"


def resolver_argumentos_job() -> dict[str, str]:
    """Resolve argumentos obrigatorios do job."""
    return sot.resolver_argumentos_job()


def inicializar_contexto() -> dict:
    """Inicializa Glue/Spark."""
    return common.inicializar_contexto_glue()


def montar_contexto_templates(args: dict[str, str]) -> dict[str, str]:
    """Monta placeholders SOT."""
    return sot.montar_contexto_templates(args)


def montar_fontes(args: dict[str, str], contexto_templates: dict[str, str]) -> tuple[dict, list[dict]]:
    """Monta execucao e fontes do SOT."""
    config_origem = common.carregar_json(CONFIG_ORIGEM_PATH)
    execucao = sot.obter_execucao(config_origem, args, contexto_templates)
    predicates = common.parsear_table_predicates(
        common.resolver_argumento_opcional("TABLE_PREDICATES")
    )
    fontes = sot.montar_fontes(execucao, predicates)
    return execucao, fontes


def carregar_fontes(contexto_glue: dict, args: dict[str, str], fontes: list[dict]) -> None:
    """Carrega fontes e registra temp views."""
    config_origem = common.carregar_json(CONFIG_ORIGEM_PATH)
    common.carregar_fontes(contexto_glue["glue_context"], fontes, args, config_origem)


def executar_consultas(contexto_glue: dict, execucao: dict) -> object:
    """Executa SQL configurado."""
    caminho_sql = common.resolver_caminho_sql(BASE_PATH, execucao["sql_path"])
    return common.executar_consultas_sql(contexto_glue["spark"], caminho_sql)


def gravar_resultado(contexto_glue: dict, args: dict[str, str], execucao: dict, df_entrada: object) -> None:
    """Adiciona colunas tecnicas e grava resultado."""
    config_destino = common.carregar_json(CONFIG_DESTINO_PATH)
    destino = sot.obter_destino(config_destino, args)
    colunas_tecnicas = sot.obter_colunas_tecnicas(execucao, args)
    df_saida = common.adicionar_colunas_tecnicas(df_entrada, colunas_tecnicas)
    common.gravar_resultado(contexto_glue["glue_context"], df_saida, destino)


def executar_fluxo_de_processamento() -> None:
    """Executa o fluxo principal do SOT."""
    args = resolver_argumentos_job()
    contexto_glue = inicializar_contexto()
    common.inicializar_rastreio_job(contexto_glue, args)

    logger.info("===Etapa 1: Montando contexto e fontes===")
    contexto_templates = montar_contexto_templates(args)
    execucao, fontes = montar_fontes(args, contexto_templates)

    logger.info("===Etapa 2: Carregando fontes===")
    carregar_fontes(contexto_glue, args, fontes)

    logger.info("===Etapa 3: Executando consultas===")
    df = executar_consultas(contexto_glue, execucao)

    logger.info("===Etapa 4: Gravando resultado===")
    gravar_resultado(contexto_glue, args, execucao, df)


if __name__ == "__main__":
    executar_fluxo_de_processamento()
