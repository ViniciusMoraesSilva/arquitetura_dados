"""Gravacao unificada no Glue Catalog/S3."""

from __future__ import annotations

import logging
from typing import Any
from urllib.parse import urlparse

import boto3
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import DataFrame


def _montar_caminho_destino_s3(configuracao_destino: dict[str, Any]) -> str:
    caminho_template = configuracao_destino["s3_destino"].rstrip("/")
    nome_tabela = configuracao_destino["nome_tabela_destino"]
    return f"{caminho_template}/{nome_tabela}/"


def _montar_caminho_pasta_csv_destino(configuracao_destino: dict[str, Any]) -> str:
    caminho_template = configuracao_destino["s3_destino"].rstrip("/")
    nome_pasta_destino = configuracao_destino["nome_pasta_destino"]
    return f"{caminho_template}/{nome_pasta_destino}/"


def _montar_caminho_arquivo_csv_destino(configuracao_destino: dict[str, Any]) -> str:
    caminho_destino = _montar_caminho_pasta_csv_destino(configuracao_destino)
    return f"{caminho_destino}{configuracao_destino['nome_arquivo_csv']}"


def _montar_caminho_temporario_csv(configuracao_destino: dict[str, Any]) -> str:
    caminho_destino = _montar_caminho_pasta_csv_destino(configuracao_destino)
    return f"{caminho_destino}_tmp/"


def _separar_bucket_e_prefixo_s3(caminho_s3: str) -> tuple[str, str]:
    caminho_parseado = urlparse(caminho_s3)
    bucket = caminho_parseado.netloc
    prefixo = caminho_parseado.path.lstrip("/")

    if not bucket:
        raise ValueError(f"Caminho S3 invalido: {caminho_s3}")

    return bucket, prefixo


def _obter_part_file_csv(
    s3_client: Any,
    bucket: str,
    prefixo_temporario: str,
) -> str:
    resposta = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefixo_temporario)
    conteudos = resposta.get("Contents", [])
    arquivos_part = [
        objeto["Key"]
        for objeto in conteudos
        if objeto["Key"].split("/")[-1].startswith("part-")
    ]

    if not arquivos_part:
        raise ValueError("Nenhum arquivo CSV temporario encontrado para promocao no S3.")

    if len(arquivos_part) > 1:
        raise ValueError(
            "Mais de um arquivo CSV temporario encontrado; esperado apenas um."
        )

    return arquivos_part[0]


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
        logger.info("Removendo conteudo existente da particao: %s", caminho_particao)
        glue_context.purge_s3_path(caminho_particao, {"retentionPeriod": 0})


def _gravar_catalogo(
    glue_context: Any,
    df_entrada: DataFrame,
    configuracao_destino: dict[str, Any],
    logger: logging.Logger,
) -> None:
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
        "Gravando catalogo database=%s tabela=%s path=%s particoes=%s",
        catalog_database,
        catalog_table_name,
        caminho_destino_s3,
        partition_keys,
    )

    dynamic_frame_saida = DynamicFrame.fromDF(
        df_entrada,
        glue_context,
        "dynamic_frame_saida",
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


def _gravar_csv_no_s3(
    glue_context: Any,
    df_entrada: DataFrame,
    configuracao_destino: dict[str, Any],
    logger: logging.Logger,
) -> None:
    caminho_temporario_csv = _montar_caminho_temporario_csv(configuracao_destino)
    caminho_arquivo_csv_destino = _montar_caminho_arquivo_csv_destino(
        configuracao_destino
    )
    output_header = configuracao_destino.get("incluir_cabecalho", True)

    logger.info(
        "Gravando CSV no S3 em diretorio temporario=%s arquivo_final=%s header=%s",
        caminho_temporario_csv,
        caminho_arquivo_csv_destino,
        output_header,
    )

    glue_context.purge_s3_path(caminho_temporario_csv, {"retentionPeriod": 0})
    df_entrada.coalesce(1).write.mode("overwrite").option(
        "header", output_header
    ).csv(caminho_temporario_csv)

    s3_client = boto3.client("s3")
    bucket_temporario, prefixo_temporario = _separar_bucket_e_prefixo_s3(
        caminho_temporario_csv
    )
    bucket_destino, chave_destino = _separar_bucket_e_prefixo_s3(
        caminho_arquivo_csv_destino
    )
    chave_origem = _obter_part_file_csv(s3_client, bucket_temporario, prefixo_temporario)

    s3_client.delete_object(Bucket=bucket_destino, Key=chave_destino)
    s3_client.copy_object(
        Bucket=bucket_destino,
        Key=chave_destino,
        CopySource={"Bucket": bucket_temporario, "Key": chave_origem},
    )
    glue_context.purge_s3_path(caminho_temporario_csv, {"retentionPeriod": 0})


def gravar_resultado_glue(
    glue_context: Any,
    df_entrada: DataFrame,
    configuracao_destino: dict[str, Any],
    logger: logging.Logger | None = None,
) -> None:
    """Grava o DataFrame final de acordo com o tipo de saida configurado."""
    logger = logger or logging.getLogger(__name__)
    tipo_saida_destino = configuracao_destino["tipo_saida_destino"].strip().lower()

    if tipo_saida_destino == "catalogo":
        _gravar_catalogo(glue_context, df_entrada, configuracao_destino, logger)
        return

    if tipo_saida_destino == "csv":
        _gravar_csv_no_s3(glue_context, df_entrada, configuracao_destino, logger)
        return

    raise ValueError("Configuracao tipo_saida_destino invalida. Use 'catalogo' ou 'csv'.")


def gravar_sor_glue(
    glue_context: Any,
    df_entrada: DataFrame,
    configuracao_destino: dict[str, Any],
    logger: logging.Logger | None = None,
) -> None:
    """Compatibilidade com o nome antigo usado pelo job SOR."""
    gravar_resultado_glue(glue_context, df_entrada, configuracao_destino, logger)
