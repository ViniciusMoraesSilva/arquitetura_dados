"""Utilitarios para leitura e execucao sequencial de consultas SQL."""

import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


def _separar_consultas_sql(conteudo_sql: str) -> list[str]:
    """Separa o conteudo SQL em consultas independentes."""
    consultas = [trecho.strip() for trecho in conteudo_sql.split(";")]
    return [consulta for consulta in consultas if consulta]


def executar_consultas_sql(
    spark: SparkSession,
    caminho_sql: str,
    prefixo_view_temp: str = "query_temp_",
) -> DataFrame:
    """Executa consultas de um arquivo SQL em sequencia no Spark."""
    caminho_arquivo = Path(caminho_sql)

    if not caminho_arquivo.exists():
        raise FileNotFoundError(f"Arquivo SQL nao encontrado: {caminho_sql}")

    conteudo_sql = caminho_arquivo.read_text(encoding="utf-8")
    consultas = _separar_consultas_sql(conteudo_sql)
    if not consultas:
        raise ValueError("Arquivo SQL sem queries executaveis.")

    ultimo_dataframe: DataFrame | None = None
    for indice, consulta in enumerate(consultas, start=1):
        nome_view_temp = f"{prefixo_view_temp}{indice}"
        logger.info(
            "Executando query temporaria %s com a view %s",
            indice,
            nome_view_temp,
        )
        try:
            ultimo_dataframe = spark.sql(consulta)
            ultimo_dataframe.createOrReplaceTempView(nome_view_temp)
        except Exception as exc:
            raise RuntimeError(
                f"Falha ao executar a consulta {indice} na view temporaria "
                f"{nome_view_temp}."
            ) from exc

    if ultimo_dataframe is None:
        raise RuntimeError("Nenhuma consulta foi executada com sucesso.")

    return ultimo_dataframe
