"""Utilitários para leitura e execução sequencial de consultas SQL."""

import logging
from pathlib import Path
from urllib.parse import urlparse

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


def _separar_consultas_sql(conteudo_sql: str) -> list[str]:
    """
    Separa o conteúdo de um arquivo SQL em consultas independentes.

    Args:
        conteudo_sql: Conteúdo bruto do arquivo SQL.

    Returns:
        Lista de consultas não vazias separadas por ``;``.
    """
    consultas = [trecho.strip() for trecho in conteudo_sql.split(";")]
    return [consulta for consulta in consultas if consulta]


def _ler_sql(caminho_sql: str) -> str:
    if caminho_sql.startswith("s3://"):
        import boto3

        parsed = urlparse(caminho_sql)
        response = boto3.client("s3").get_object(
            Bucket=parsed.netloc,
            Key=parsed.path.lstrip("/"),
        )
        return response["Body"].read().decode("utf-8")

    caminho_arquivo = Path(caminho_sql)
    if not caminho_arquivo.exists():
        raise FileNotFoundError(f"Arquivo SQL nao encontrado: {caminho_sql}")
    return caminho_arquivo.read_text(encoding="utf-8")


def executar_consultas_sql(
    spark: SparkSession,
    caminho_sql: str,
    prefixo_view_temp: str = "query_temp_",
    template_context: dict[str, str] | None = None,
) -> DataFrame:
    """
    Executa sequencialmente as consultas de um arquivo SQL no Spark.

    Cada consulta é registrada como temp view intermediária com o prefixo
    informado, permitindo que consultas posteriores dependam das anteriores.

    Args:
        spark: Sessão Spark utilizada para executar as consultas.
        caminho_sql: Caminho do arquivo SQL a ser processado.
        prefixo_view_temp: Prefixo das temp views intermediárias.

    Returns:
        DataFrame resultante da última consulta executada.
    """
    conteudo_sql = _ler_sql(caminho_sql)
    for chave, valor in (template_context or {}).items():
        conteudo_sql = conteudo_sql.replace(f"${{{chave}}}", str(valor))
        conteudo_sql = conteudo_sql.replace(f"{{{chave}}}", str(valor))
    consultas = _separar_consultas_sql(conteudo_sql)

    if not consultas:
        raise ValueError("Arquivo SQL sem queries executaveis.")

    ultimo_dataframe: DataFrame | None = None

    for indice, consulta in enumerate(consultas, start=1):
        nome_view_temp = f"{prefixo_view_temp}{indice}"
        logger.info(
            "Executando query temporaria %s com a view %s e query SQL: %s",
            indice,
            nome_view_temp,
            consulta,
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
