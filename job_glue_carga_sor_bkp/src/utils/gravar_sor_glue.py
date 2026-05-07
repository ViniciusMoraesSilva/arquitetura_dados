"""Gravacao de versoes SOR no Glue Catalog e S3."""

from __future__ import annotations

import logging
from typing import Any

from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import DataFrame


def _montar_caminho_destino_s3(configuracao_destino: dict[str, Any]) -> str:
    caminho_template = configuracao_destino["s3_destino"].rstrip("/")
    nome_tabela = configuracao_destino["nome_tabela_destino"]
    return f"{caminho_template}/{nome_tabela}/"


def _obter_partition_keys_destino(
    df_entrada: DataFrame,
    configuracao_destino: dict[str, Any],
) -> list[str]:
    partition_keys = [
        coluna.strip()
        for coluna in configuracao_destino["particao_tabela_destino"]
        if coluna.strip()
    ]
    if not partition_keys:
        raise ValueError(
            "A configuracao particao_tabela_destino deve conter ao menos uma coluna."
        )

    colunas_invalidas = [
        coluna for coluna in partition_keys if coluna not in df_entrada.columns
    ]
    if colunas_invalidas:
        raise ValueError(
            "Colunas de particionamento nao encontradas no DataFrame final: "
            + ", ".join(colunas_invalidas)
        )

    return partition_keys


def _purge_particoes_destino(
    glue_context: Any,
    df_entrada: DataFrame,
    caminho_destino_s3: str,
    partition_keys: list[str],
    logger: logging.Logger,
) -> None:
    colunas_particao = df_entrada.select(*partition_keys).distinct().collect()

    for linha_particao in colunas_particao:
        componentes = [
            f"{partition_key}={linha_particao[partition_key]}"
            for partition_key in partition_keys
        ]
        caminho_particao = f"{caminho_destino_s3}{'/'.join(componentes)}/"
        logger.info("Removendo conteudo existente da particao SOR: %s", caminho_particao)
        glue_context.purge_s3_path(caminho_particao, {"retentionPeriod": 0})


def gravar_sor_glue(
    glue_context: Any,
    df_entrada: DataFrame,
    configuracao_destino: dict[str, Any],
    logger: logging.Logger | None = None,
) -> None:
    """Grava o DataFrame final da ingestao na camada SOR."""
    logger = logger or logging.getLogger(__name__)
    partition_keys = _obter_partition_keys_destino(df_entrada, configuracao_destino)
    caminho_destino_s3 = _montar_caminho_destino_s3(configuracao_destino)
    catalog_database = configuracao_destino["nome_database_destino"]
    catalog_table_name = configuracao_destino["nome_tabela_destino"]

    _purge_particoes_destino(
        glue_context,
        df_entrada,
        caminho_destino_s3,
        partition_keys,
        logger,
    )

    logger.info(
        "Gravando SOR database=%s tabela=%s path=%s particoes=%s",
        catalog_database,
        catalog_table_name,
        caminho_destino_s3,
        partition_keys,
    )

    dynamic_frame_saida = DynamicFrame.fromDF(
        df_entrada,
        glue_context,
        "dynamic_frame_sor",
    )
    sink = glue_context.getSink(
        connection_type="s3",
        path=caminho_destino_s3,
        enableUpdateCatalog=True,
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=partition_keys,
        groupFiles=configuracao_destino["group_files"],
        groupSize=str(configuracao_destino["group_size_bytes"]),
    )
    sink.setFormat(
        "parquet",
        useGlueParquetWriter=configuracao_destino["use_glue_parquet_writer"],
        compression=configuracao_destino["compression"],
        blockSize=int(configuracao_destino["block_size_bytes"]),
    )
    sink.setCatalogInfo(
        catalogDatabase=catalog_database,
        catalogTableName=catalog_table_name,
    )
    sink.writeFrame(dynamic_frame_saida)
