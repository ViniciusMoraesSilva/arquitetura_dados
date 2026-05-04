"""Funções auxiliares para gravação de resultados no destino de dados."""

from __future__ import annotations

import logging
from typing import Any
from urllib.parse import urlparse

import boto3
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import DataFrame

from .config_destino_dados import (
    obter_configuracao_destino,
    obter_configuracao_destino_dados,
)


def _obter_partition_keys_destino(
    df_entrada: DataFrame, configuracao_catalogo_glue: dict
) -> list[str]:
    """
    Valida e retorna as chaves de partição configuradas para a gravação.

    Args:
        df_entrada: DataFrame final que será gravado.
        configuracao_catalogo_glue: Configuração final do catálogo Glue.

    Returns:
        Lista de colunas de partição válidas no DataFrame.
    """
    partition_keys = [
        coluna.strip()
        for coluna in configuracao_catalogo_glue["particao_tabela_destino"]
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


def _montar_caminho_destino_s3(configuracao_catalogo_glue: dict) -> str:
    """
    Monta o caminho S3 base de destino para a tabela.

    Args:
        configuracao_catalogo_glue: Configuração do catálogo Glue.

    Returns:
        Caminho S3 base da gravação da tabela.
    """
    caminho_template = configuracao_catalogo_glue["s3_destino"].rstrip("/")
    nome_tabela = configuracao_catalogo_glue["nome_tabela_destino"]

    return f"{caminho_template}/{nome_tabela}/"


def _montar_caminho_pasta_csv_destino(configuracao_catalogo_glue: dict) -> str:
    """
    Monta o caminho S3 base da pasta de destino do CSV.

    Args:
        configuracao_catalogo_glue: Configuração do destino de dados.

    Returns:
        Caminho S3 base da pasta do CSV.
    """
    caminho_template = configuracao_catalogo_glue["s3_destino"].rstrip("/")
    nome_pasta_destino = configuracao_catalogo_glue["nome_pasta_destino"]

    return f"{caminho_template}/{nome_pasta_destino}/"


def _montar_caminho_arquivo_csv_destino(configuracao_catalogo_glue: dict) -> str:
    """
    Monta o caminho S3 final do arquivo CSV de saída.

    Args:
        configuracao_catalogo_glue: Configuração do catálogo Glue.

    Returns:
        Caminho S3 completo do arquivo CSV final.
    """
    caminho_destino_tabela = _montar_caminho_pasta_csv_destino(configuracao_catalogo_glue)
    nome_arquivo = configuracao_catalogo_glue["nome_arquivo_csv"]

    return f"{caminho_destino_tabela}{nome_arquivo}"


def _montar_caminho_temporario_csv(configuracao_catalogo_glue: dict) -> str:
    """
    Monta o diretório temporário usado na escrita do CSV.

    Args:
        configuracao_catalogo_glue: Configuração do catálogo Glue.

    Returns:
        Caminho S3 do diretório temporário de saída CSV.
    """
    caminho_destino_tabela = _montar_caminho_pasta_csv_destino(configuracao_catalogo_glue)
    return f"{caminho_destino_tabela}_tmp/"


def _separar_bucket_e_prefixo_s3(caminho_s3: str) -> tuple[str, str]:
    """
    Separa bucket e prefixo de um caminho S3.

    Args:
        caminho_s3: Caminho completo no formato ``s3://bucket/prefixo``.

    Returns:
        Tupla com bucket e prefixo.
    """
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
    """
    Localiza o arquivo ``part-*`` gerado na escrita temporária do CSV.

    Args:
        s3_client: Cliente boto3 do S3.
        bucket: Bucket do S3.
        prefixo_temporario: Prefixo temporário onde o Spark gravou os artefatos.

    Returns:
        Chave S3 do único arquivo ``part-*`` encontrado.
    """
    resposta = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefixo_temporario)
    conteudos = resposta.get("Contents", [])
    arquivos_part = [
        objeto["Key"]
        for objeto in conteudos
        if objeto["Key"].split("/")[-1].startswith("part-")
    ]

    if not arquivos_part:
        raise ValueError(
            "Nenhum arquivo CSV temporario encontrado para promocao no S3."
        )

    if len(arquivos_part) > 1:
        raise ValueError(
            "Mais de um arquivo CSV temporario encontrado; esperado apenas um."
        )

    return arquivos_part[0]


def _purge_particoes_destino(
    glue_context: Any,
    df_entrada: DataFrame,
    caminho_destino_s3: str,
    partition_keys: list[str],
    logger: logging.Logger,
) -> None:
    """
    Remove o conteúdo das partições que serão regravadas no destino.

    Args:
        glue_context: Contexto Glue utilizado para o purge no S3.
        df_entrada: DataFrame final que será persistido.
        caminho_destino_s3: Caminho S3 base de destino.
        partition_keys: Lista das chaves de partição da tabela.
        logger: Logger utilizado na execução.
    """
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
    configuracao_catalogo_glue: dict,
    configuracao_destino: dict,
    logger: logging.Logger,
) -> None:
    """
    Grava o DataFrame em Parquet e atualiza o Glue Catalog.
    """
    group_files = configuracao_destino["group_files"]
    group_size_bytes = str(configuracao_destino["group_size_bytes"])
    use_glue_parquet_writer = configuracao_destino["use_glue_parquet_writer"]
    compression = configuracao_destino["compression"]
    block_size_bytes = int(configuracao_destino["block_size_bytes"])
    partition_keys = _obter_partition_keys_destino(
        df_entrada,
        configuracao_catalogo_glue,
    )
    caminho_destino_s3 = _montar_caminho_destino_s3(configuracao_catalogo_glue)
    catalog_database = configuracao_catalogo_glue["nome_database_destino"]
    catalog_table_name = configuracao_catalogo_glue["nome_tabela_destino"]

    _purge_particoes_destino(
        glue_context,
        df_entrada,
        caminho_destino_s3,
        partition_keys,
        logger,
    )

    logger.info(
        "Gravando dados no Glue Catalog database=%s tabela=%s path=%s particoes=%s",
        catalog_database,
        catalog_table_name,
        caminho_destino_s3,
        partition_keys,
    )

    dynamic_frame_saida = DynamicFrame.fromDF(
        df_entrada, glue_context, "dynamic_frame_saida"
    )
    sink = glue_context.getSink(
        connection_type="s3",
        path=caminho_destino_s3,
        enableUpdateCatalog=True,
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=partition_keys,
        groupFiles=group_files,
        groupSize=group_size_bytes,
    )
    sink.setFormat(
        "parquet",
        useGlueParquetWriter=use_glue_parquet_writer,
        compression=compression,
        blockSize=block_size_bytes,
    )
    sink.setCatalogInfo(
        catalogDatabase=catalog_database,
        catalogTableName=catalog_table_name,
    )
    sink.writeFrame(dynamic_frame_saida)


def _gravar_csv_no_s3(
    glue_context: Any,
    df_entrada: DataFrame,
    configuracao_catalogo_glue: dict,
    logger: logging.Logger,
) -> None:
    """
    Grava o resultado final em CSV no S3 com nome de arquivo fixo.
    """
    caminho_temporario_csv = _montar_caminho_temporario_csv(configuracao_catalogo_glue)
    caminho_arquivo_csv_destino = _montar_caminho_arquivo_csv_destino(
        configuracao_catalogo_glue
    )
    output_header = configuracao_catalogo_glue.get("incluir_cabecalho", True)

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


def gravar_catalogo_glue(
    glue_context: Any,
    df_entrada: DataFrame,
    args: dict,
    logger: logging.Logger | None = None,
) -> None:
    """
    Grava o DataFrame final no destino configurado.

    Args:
        glue_context: Contexto Glue utilizado na escrita.
        df_entrada: DataFrame final que será persistido.
        args: Argumentos do job usados para resolver o destino.
        logger: Logger opcional da execução.
    """
    logger = logger or logging.getLogger(__name__)
    configuracao_catalogo_glue = obter_configuracao_destino_dados(args)
    tipo_saida_destino = configuracao_catalogo_glue["tipo_saida_destino"].strip().lower()

    if tipo_saida_destino == "catalogo":
        configuracao_destino = obter_configuracao_destino()
        _gravar_catalogo(
            glue_context=glue_context,
            df_entrada=df_entrada,
            configuracao_catalogo_glue=configuracao_catalogo_glue,
            configuracao_destino=configuracao_destino,
            logger=logger,
        )
        return

    if tipo_saida_destino == "csv":
        _gravar_csv_no_s3(
            glue_context=glue_context,
            df_entrada=df_entrada,
            configuracao_catalogo_glue=configuracao_catalogo_glue,
            logger=logger,
        )
        return

    raise ValueError(
        "Configuracao tipo_saida_destino invalida. Use 'catalogo' ou 'csv'."
    )
