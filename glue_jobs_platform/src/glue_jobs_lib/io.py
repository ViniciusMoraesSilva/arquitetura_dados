"""Leitura e escrita em Glue/S3."""

from __future__ import annotations

from typing import Any
from urllib.parse import urlparse

from .models import DestinationConfig, SourceConfig


def load_source(glue_context: Any, source: SourceConfig, read_options: dict[str, Any]) -> Any:
    """Carrega uma fonte catalogo ou CSV e registra temp view."""
    if source.source_type == "catalogo":
        params = {
            "database": source.options["database_origem"],
            "table_name": source.options["tabela_origem"],
            "useCatalogSchema": source.options.get("use_catalog", True),
            "additional_options": {
                "groupFiles": read_options["group_files"],
                "groupSize": str(read_options["group_size_bytes"]),
            },
        }
        if source.predicate:
            params["push_down_predicate"] = source.predicate
        df = glue_context.create_data_frame.from_catalog(**params)
    elif source.source_type == "csv":
        dynamic_frame = glue_context.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={
                "paths": [source.options["caminho_s3_origem"]],
                "groupFiles": read_options["group_files"],
                "groupSize": str(read_options["group_size_bytes"]),
            },
            format="csv",
            format_options={
                "withHeader": source.options.get("header", True),
                "separator": source.options.get("delimiter", ","),
                "quoteChar": source.options.get("quote", '"'),
                "escaper": source.options.get("escape", "\\"),
                "inferSchema": source.options.get("infer_schema", True),
                "encoding": source.options.get("encoding", "UTF-8"),
            },
        )
        df = dynamic_frame.toDF()
        if source.predicate:
            df = df.filter(source.predicate)
    else:
        raise ValueError("source_type invalido. Use 'catalogo' ou 'csv'.")

    if source.duplicate_fields is not None:
        if source.duplicate_fields == ["*"]:
            df = df.dropDuplicates()
        else:
            df = df.dropDuplicates(source.duplicate_fields)

    df.createOrReplaceTempView(source.view_name)
    return df


def _partition_keys(df: Any, destination: DestinationConfig) -> list[str]:
    keys = [
        key.strip()
        for key in destination.options.get("particao_tabela_destino", [])
        if key.strip()
    ]
    if not keys:
        raise ValueError("Destino catalogo exige particao_tabela_destino.")
    invalid = [key for key in keys if key not in df.columns]
    if invalid:
        raise ValueError(
            "Colunas de particionamento nao encontradas no DataFrame final: "
            + ", ".join(invalid)
        )
    return keys


def write_result(glue_context: Any, df: Any, destination: DestinationConfig) -> None:
    """Grava resultado em catalogo ou CSV."""
    if destination.output_type == "catalogo":
        _write_catalog(glue_context, df, destination)
        return
    if destination.output_type == "csv":
        _write_csv(glue_context, df, destination)
        return
    raise ValueError("output_type invalido. Use 'catalogo' ou 'csv'.")


def _write_catalog(glue_context: Any, df: Any, destination: DestinationConfig) -> None:
    from awsglue.dynamicframe import DynamicFrame

    options = destination.options
    partition_keys = _partition_keys(df, destination)
    path = f"{options['s3_destino'].rstrip('/')}/{options['nome_tabela_destino']}/"

    for row in df.select(*partition_keys).distinct().collect():
        components = [f"{key}={row[key]}" for key in partition_keys]
        glue_context.purge_s3_path(f"{path}{'/'.join(components)}/", {"retentionPeriod": 0})

    dynamic_frame = DynamicFrame.fromDF(df, glue_context, "dynamic_frame_saida")
    sink = glue_context.getSink(
        connection_type="s3",
        path=path,
        enableUpdateCatalog=True,
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=partition_keys,
        groupFiles=options["group_files"],
        groupSize=str(options["group_size_bytes"]),
    )
    sink.setFormat(
        "parquet",
        useGlueParquetWriter=options["use_glue_parquet_writer"],
        compression=options["compression"],
        blockSize=int(options["block_size_bytes"]),
    )
    sink.setCatalogInfo(
        catalogDatabase=options["nome_database_destino"],
        catalogTableName=options["nome_tabela_destino"],
    )
    sink.writeFrame(dynamic_frame)


def _split_s3(path: str) -> tuple[str, str]:
    parsed = urlparse(path)
    if not parsed.netloc:
        raise ValueError(f"Caminho S3 invalido: {path}")
    return parsed.netloc, parsed.path.lstrip("/")


def _write_csv(glue_context: Any, df: Any, destination: DestinationConfig) -> None:
    import boto3

    options = destination.options
    base_path = f"{options['s3_destino'].rstrip('/')}/{options['nome_pasta_destino']}/"
    temp_path = f"{base_path}_tmp/"
    final_path = f"{base_path}{options['nome_arquivo_csv']}"

    glue_context.purge_s3_path(temp_path, {"retentionPeriod": 0})
    df.coalesce(1).write.mode("overwrite").option(
        "header", options.get("incluir_cabecalho", True)
    ).csv(temp_path)

    s3_client = boto3.client("s3")
    temp_bucket, temp_prefix = _split_s3(temp_path)
    final_bucket, final_key = _split_s3(final_path)
    response = s3_client.list_objects_v2(Bucket=temp_bucket, Prefix=temp_prefix)
    part_files = [
        obj["Key"]
        for obj in response.get("Contents", [])
        if obj["Key"].split("/")[-1].startswith("part-")
    ]
    if len(part_files) != 1:
        raise ValueError("Esperado exatamente um arquivo part-* para CSV.")
    s3_client.delete_object(Bucket=final_bucket, Key=final_key)
    s3_client.copy_object(
        Bucket=final_bucket,
        Key=final_key,
        CopySource={"Bucket": temp_bucket, "Key": part_files[0]},
    )
    glue_context.purge_s3_path(temp_path, {"retentionPeriod": 0})
