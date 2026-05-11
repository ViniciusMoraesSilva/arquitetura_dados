"""Funcoes comuns para jobs Glue procedurais."""

from __future__ import annotations

import json
import re
import sys
from datetime import datetime
from pathlib import Path
from string import Formatter
from typing import Any
from urllib.parse import urlparse

DEFAULT_PIPELINE_CONTROL_TABLE = "pipeline_control"


def resolver_argumentos_obrigatorios(nomes_argumentos: list[str]) -> dict[str, str]:
    """Resolve argumentos obrigatorios do Glue."""
    from awsglue.utils import getResolvedOptions

    return getResolvedOptions(sys.argv, nomes_argumentos)


def resolver_argumento_opcional(nome_argumento: str) -> str | None:
    """Resolve argumento opcional do Glue."""
    try:
        from awsglue.utils import getResolvedOptions

        return getResolvedOptions(sys.argv, [nome_argumento])[nome_argumento]
    except Exception:
        return None


def inicializar_contexto_glue() -> dict[str, Any]:
    """Inicializa Spark, GlueContext e Job."""
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from pyspark.context import SparkContext

    sc = SparkContext()
    glue_context = GlueContext(sc)
    return {
        "sc": sc,
        "glue_context": glue_context,
        "spark": glue_context.spark_session,
        "job": Job(glue_context),
    }


def inicializar_rastreio_job(contexto_glue: dict[str, Any], args: dict[str, str]) -> None:
    """Inicializa o rastreio do job no Glue."""
    contexto_glue["job"].init(args["JOB_NAME"], args)


def carregar_json(caminho: str | Path) -> dict[str, Any]:
    """Carrega arquivo JSON."""
    caminho_config = Path(caminho)
    if not caminho_config.exists():
        raise FileNotFoundError(f"Arquivo de configuracao nao encontrado: {caminho_config}")
    return json.loads(caminho_config.read_text(encoding="utf-8"))


def selecionar_unico(
    itens: list[dict[str, Any]],
    filtros: dict[str, str],
    descricao: str,
) -> dict[str, Any]:
    """Seleciona exatamente um item por filtros."""
    encontrados = [
        item for item in itens if all(item.get(campo) == valor for campo, valor in filtros.items())
    ]
    if not encontrados:
        filtros_texto = ", ".join(f"{campo}={valor}" for campo, valor in filtros.items())
        raise ValueError(f"{descricao} nao encontrado para {filtros_texto}.")
    if len(encontrados) > 1:
        raise ValueError(f"Mais de um {descricao} encontrado.")
    return encontrados[0]


def montar_contexto_data(data_ref: str) -> dict[str, str]:
    """Monta placeholders de data a partir de DATA_REF."""
    if len(data_ref) == 6:
        data_ref_datetime = datetime.strptime(data_ref, "%Y%m")
        ano_mes_data_ref = data_ref
    elif len(data_ref) == 8:
        data_ref_datetime = datetime.strptime(data_ref, "%Y%m%d")
        ano_mes_data_ref = data_ref[:6]
    else:
        raise ValueError("DATA_REF invalida. Use os formatos YYYYMM ou YYYYMMDD.")

    primeiro_dia_mes = data_ref_datetime.replace(day=1)
    if primeiro_dia_mes.month == 1:
        ano_mes_anterior = f"{primeiro_dia_mes.year - 1}12"
    else:
        ano_mes_anterior = f"{primeiro_dia_mes.year}{primeiro_dia_mes.month - 1:02d}"

    return {
        "DATA_REF": data_ref,
        "DATA_ATUAL": datetime.now().strftime("%Y%m%d"),
        "ANO_DATA_REF": str(data_ref_datetime.year),
        "MES_DATA_REF": f"{data_ref_datetime.month:02d}",
        "ANO_MES_DATA_REF": ano_mes_data_ref,
        "MES_ANTERIOR": ano_mes_anterior[-2:],
        "ANO_MES_ANTERIOR": ano_mes_anterior,
    }


def normalizar_nome_placeholder(valor: str) -> str:
    """Normaliza nomes dinamicos para placeholder."""
    return re.sub(r"[^A-Z0-9_]", "_", valor.upper())


def config_usa_placeholder(valor: Any, nome_placeholder: str) -> bool:
    """Verifica se uma estrutura usa um placeholder."""
    if isinstance(valor, dict):
        return any(config_usa_placeholder(item, nome_placeholder) for item in valor.values())
    if isinstance(valor, list):
        return any(config_usa_placeholder(item, nome_placeholder) for item in valor)
    if not isinstance(valor, str):
        return False
    placeholders = {
        nome for _, nome, _, _ in Formatter().parse(valor) if nome is not None
    }
    return nome_placeholder in placeholders


def obter_metadata_processo(
    dynamodb_resource: Any,
    process_id: str,
    table_name: str = DEFAULT_PIPELINE_CONTROL_TABLE,
) -> dict[str, Any]:
    """Busca metadata do processo por PROCESS_ID."""
    tabela = dynamodb_resource.Table(table_name)
    resposta = tabela.get_item(
        Key={
            "PK": f"PROCESS#{process_id}",
            "SK": "METADATA",
        }
    )
    metadata = resposta.get("Item")
    if not metadata:
        raise ValueError(f"Metadata de processo nao encontrada para PROCESS_ID={process_id}.")
    if str(metadata.get("process_id")) != str(process_id):
        raise ValueError("Metadata retornada nao corresponde ao PROCESS_ID solicitado.")
    if not metadata.get("process_name"):
        raise ValueError(f"Metadata do PROCESS_ID={process_id} nao possui process_name.")
    if not metadata.get("data_referencia"):
        raise ValueError(f"Metadata do PROCESS_ID={process_id} nao possui data_referencia.")
    return metadata


def montar_contexto_processo(metadata: dict[str, Any]) -> dict[str, str]:
    """Monta contexto de templates a partir da metadata do processo."""
    return {
        "PROCESS_ID": str(metadata["process_id"]),
        "PROCESS_NAME": str(metadata["process_name"]),
        **montar_contexto_data(str(metadata["data_referencia"])),
    }


def obter_input_locks_processo(
    dynamodb_resource: Any,
    process_id: str,
    table_name: str = DEFAULT_PIPELINE_CONTROL_TABLE,
) -> dict[str, dict[str, Any]]:
    """Busca INPUT_LOCKs do processo, indexados por sor_table_name."""
    tabela = dynamodb_resource.Table(table_name)
    parametros_query = {
        "KeyConditionExpression": "#pk = :pk AND begins_with(#sk, :prefixo_sk)",
        "ExpressionAttributeNames": {"#pk": "PK", "#sk": "SK"},
        "ExpressionAttributeValues": {
            ":pk": f"PROCESS#{process_id}",
            ":prefixo_sk": "INPUT_LOCK#",
        },
    }
    itens = []
    while True:
        resposta = tabela.query(**parametros_query)
        itens.extend(resposta.get("Items", []))
        last_key = resposta.get("LastEvaluatedKey")
        if not last_key:
            break
        parametros_query["ExclusiveStartKey"] = last_key

    locks = {}
    for item in itens:
        sor_table_name = item.get("sor_table_name")
        ingestion_id = item.get("ingestion_id")
        if not sor_table_name or not ingestion_id:
            raise ValueError(
                "INPUT_LOCK invalido para "
                f"PROCESS_ID={process_id}: sor_table_name e ingestion_id sao obrigatorios."
            )
        locks[str(sor_table_name)] = item
    return locks


def resolver_template(campo: str, valor: Any, contexto: dict[str, str]) -> Any:
    """Resolve templates em dict/list/string e falha para placeholder desconhecido."""
    if isinstance(valor, dict):
        return {chave: resolver_template(chave, item, contexto) for chave, item in valor.items()}
    if isinstance(valor, list):
        return [resolver_template(campo, item, contexto) for item in valor]
    if not isinstance(valor, str):
        return valor

    placeholders = [
        nome for _, nome, _, _ in Formatter().parse(valor) if nome is not None
    ]
    desconhecidos = [nome for nome in placeholders if nome not in contexto]
    if desconhecidos:
        raise ValueError(
            f"Placeholder desconhecido no campo {campo}: " + ", ".join(desconhecidos)
        )
    return valor.format(**contexto)


def resolver_execucao_com_input_locks(
    execucao: dict[str, Any],
    contexto: dict[str, str],
    input_locks: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """Resolve execucao usando INPUT_LOCK por fonte quando INGESTION_ID for necessario."""
    execucao_base = {chave: valor for chave, valor in execucao.items() if chave != "fontes"}
    execucao_resolvida = resolver_template("execucao", execucao_base, contexto)
    fontes_resolvidas = []

    for fonte in execucao.get("fontes", []):
        contexto_fonte = {**contexto}
        if config_usa_placeholder(fonte, "INGESTION_ID"):
            tabela_origem = str(fonte["tabela_origem"])
            lock = input_locks.get(tabela_origem)
            if not lock:
                raise ValueError(
                    "INPUT_LOCK nao encontrado para "
                    f"PROCESS_ID={contexto['PROCESS_ID']}, tabela_origem={tabela_origem}."
                )
            contexto_fonte["INGESTION_ID"] = str(lock["ingestion_id"])
        fontes_resolvidas.append(resolver_template("fonte", fonte, contexto_fonte))

    execucao_resolvida["fontes"] = fontes_resolvidas
    return execucao_resolvida


def parsear_table_predicates(predicates_str: str | None) -> dict[str, str]:
    """Parseia TABLE_PREDICATES no formato nome_fonte::predicate."""
    if not predicates_str:
        return {}

    resultado = {}
    for item in predicates_str.split(";"):
        item = item.strip()
        if not item or "::" not in item:
            continue
        nome_fonte, predicate = item.split("::", 1)
        nome_fonte = nome_fonte.strip()
        predicate = predicate.strip()
        if nome_fonte and predicate:
            resultado[nome_fonte] = predicate
    return resultado


def obter_nome_fonte(fonte: dict[str, Any]) -> str:
    """Resolve a chave logica da fonte."""
    return str(fonte["tabela_origem"])


def resolver_predicate_fonte(
    fonte: dict[str, Any],
    predicates_argumento: dict[str, str] | None = None,
    predicates_ddb: dict[str, str] | None = None,
) -> str | None:
    """Resolve predicate por prioridade argumento, DynamoDB e config."""
    nome_fonte = obter_nome_fonte(fonte)
    predicates_argumento = predicates_argumento or {}
    predicates_ddb = predicates_ddb or {}

    if nome_fonte in predicates_argumento:
        return predicates_argumento[nome_fonte]
    if nome_fonte in predicates_ddb:
        return predicates_ddb[nome_fonte]
    return fonte.get("filtro_origem")


def obter_configuracao_leitura_origem(config_origem: dict[str, Any]) -> dict[str, Any]:
    """Retorna parametros tecnicos de leitura do catalogo."""
    catalogo = config_origem.get("catalogo", {})
    campos_obrigatorios = ["group_files", "group_size_bytes"]
    faltantes = [campo for campo in campos_obrigatorios if campo not in catalogo]
    if faltantes:
        raise ValueError("Campos obrigatorios ausentes em catalogo: " + ", ".join(faltantes))
    return {campo: catalogo[campo] for campo in campos_obrigatorios}


def montar_fontes(
    execucao: dict[str, Any],
    predicates_argumento: dict[str, str] | None = None,
    predicates_ddb: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    """Monta fontes finais com predicate resolvido."""
    fontes = []
    for fonte in execucao.get("fontes", []):
        fonte_final = {**fonte}
        fonte_final["predicate_final"] = resolver_predicate_fonte(
            fonte_final,
            predicates_argumento,
            predicates_ddb,
        )
        fontes.append(fonte_final)
    return fontes


def carregar_fonte(
    glue_context: Any,
    fonte: dict[str, Any],
    args: dict[str, str],
    config_leitura: dict[str, Any],
) -> Any:
    """Carrega uma fonte catalogo ou CSV e registra temp view."""
    tipo_origem = fonte.get("tipo_origem", "catalogo").strip().lower()
    nome_view = obter_nome_fonte(fonte)
    predicate = fonte.get("predicate_final")

    if tipo_origem == "catalogo":
        database_origem = (
            "dblocal"
            if args["AMBIENTE"] != "prd" and args["DBLOCAL"].lower() == "true"
            else fonte["database_origem"]
        )
        parametros_leitura = {
            "database": database_origem,
            "table_name": fonte["tabela_origem"],
            "useCatalogSchema": fonte.get("use_catalog", True),
            "additional_options": {
                "groupFiles": config_leitura["group_files"],
                "groupSize": str(config_leitura["group_size_bytes"]),
            },
        }
        if predicate:
            parametros_leitura["push_down_predicate"] = predicate
        df = glue_context.create_data_frame.from_catalog(**parametros_leitura)
    elif tipo_origem == "csv":
        dynamic_frame = glue_context.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={
                "paths": [fonte["caminho_s3_origem"]],
                "groupFiles": config_leitura["group_files"],
                "groupSize": str(config_leitura["group_size_bytes"]),
            },
            format="csv",
            format_options={
                "withHeader": fonte.get("header", True),
                "separator": fonte.get("delimiter", ","),
                "quoteChar": fonte.get("quote", '"'),
                "escaper": fonte.get("escape", "\\"),
                "inferSchema": fonte.get("infer_schema", True),
                "encoding": fonte.get("encoding", "UTF-8"),
            },
        )
        df = dynamic_frame.toDF()
        if predicate:
            df = df.filter(predicate)
    else:
        raise ValueError("tipo_origem invalido. Use 'catalogo' ou 'csv'.")

    campos_duplicidade = fonte.get("campos_duplicidade")
    if campos_duplicidade is not None:
        if campos_duplicidade == ["*"]:
            df = df.dropDuplicates()
        else:
            df = df.dropDuplicates(campos_duplicidade)

    df.createOrReplaceTempView(nome_view)
    return df


def carregar_fontes(
    glue_context: Any,
    fontes: list[dict[str, Any]],
    args: dict[str, str],
    config_origem: dict[str, Any],
) -> None:
    """Carrega todas as fontes configuradas."""
    config_leitura = obter_configuracao_leitura_origem(config_origem)
    for fonte in fontes:
        carregar_fonte(glue_context, fonte, args, config_leitura)


def separar_consultas_sql(conteudo_sql: str) -> list[str]:
    """Separa SQL em consultas executaveis."""
    return [trecho.strip() for trecho in conteudo_sql.split(";") if trecho.strip()]


def executar_consultas_sql(spark: Any, caminho_sql: str | Path) -> Any:
    """Executa SQL sequencialmente e retorna o ultimo DataFrame."""
    caminho = Path(caminho_sql)
    if not caminho.exists():
        raise FileNotFoundError(f"Arquivo SQL nao encontrado: {caminho}")

    consultas = separar_consultas_sql(caminho.read_text(encoding="utf-8"))
    if not consultas:
        raise ValueError("Arquivo SQL sem queries executaveis.")

    ultimo_dataframe = None
    for indice, consulta in enumerate(consultas, start=1):
        ultimo_dataframe = spark.sql(consulta)
        ultimo_dataframe.createOrReplaceTempView(f"query_temp_{indice}")

    return ultimo_dataframe


def resolver_caminho_sql(base_path: str | Path, sql_path: str) -> str:
    """Resolve caminho SQL absoluto ou relativo ao job."""
    caminho = Path(sql_path)
    if not caminho.is_absolute():
        caminho = Path(base_path) / caminho
    return str(caminho)


def adicionar_colunas_tecnicas(df: Any, colunas_tecnicas: dict[str, str]) -> Any:
    """Adiciona colunas tecnicas constantes ao DataFrame."""
    from pyspark.sql.functions import lit

    df_saida = df
    for coluna, valor in colunas_tecnicas.items():
        df_saida = df_saida.withColumn(coluna, lit(valor))
    return df_saida


def montar_destino_final(
    config_destino: dict[str, Any],
    destino: dict[str, Any],
    args: dict[str, str],
) -> dict[str, Any]:
    """Consolida destino com ambiente e parametros tecnicos."""
    tipo_saida = destino.get("tipo_saida_destino", "catalogo").strip().lower()
    ambiente = config_destino["ambientes"][args["AMBIENTE"]]

    if tipo_saida == "catalogo":
        return {
            **config_destino["catalogo"],
            **ambiente,
            **destino,
            "tipo_saida_destino": tipo_saida,
        }
    if tipo_saida == "csv":
        return {
            **config_destino.get("csv", {}),
            **ambiente,
            **destino,
            "tipo_saida_destino": tipo_saida,
        }
    raise ValueError("tipo_saida_destino invalido. Use 'catalogo' ou 'csv'.")


def obter_partition_keys_destino(df: Any, destino: dict[str, Any]) -> list[str]:
    """Valida e retorna particoes do destino."""
    partition_keys = [
        coluna.strip()
        for coluna in destino.get("particao_tabela_destino", [])
        if coluna.strip()
    ]
    if not partition_keys:
        raise ValueError("A configuracao particao_tabela_destino deve conter colunas.")

    colunas_invalidas = [coluna for coluna in partition_keys if coluna not in df.columns]
    if colunas_invalidas:
        raise ValueError(
            "Colunas de particionamento nao encontradas no DataFrame final: "
            + ", ".join(colunas_invalidas)
        )
    return partition_keys


def gravar_catalogo(glue_context: Any, df: Any, destino: dict[str, Any]) -> None:
    """Grava em Glue Catalog/Parquet."""
    from awsglue.dynamicframe import DynamicFrame

    partition_keys = obter_partition_keys_destino(df, destino)
    caminho_s3 = f"{destino['s3_destino'].rstrip('/')}/{destino['nome_tabela_destino']}/"

    for linha in df.select(*partition_keys).distinct().collect():
        componentes = [f"{coluna}={linha[coluna]}" for coluna in partition_keys]
        glue_context.purge_s3_path(f"{caminho_s3}{'/'.join(componentes)}/", {"retentionPeriod": 0})

    dynamic_frame = DynamicFrame.fromDF(df, glue_context, "dynamic_frame_saida")
    sink = glue_context.getSink(
        connection_type="s3",
        path=caminho_s3,
        enableUpdateCatalog=True,
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=partition_keys,
        groupFiles=destino["group_files"],
        groupSize=str(destino["group_size_bytes"]),
    )
    sink.setFormat(
        "parquet",
        useGlueParquetWriter=destino["use_glue_parquet_writer"],
        compression=destino["compression"],
        blockSize=int(destino["block_size_bytes"]),
    )
    sink.setCatalogInfo(
        catalogDatabase=destino["nome_database_destino"],
        catalogTableName=destino["nome_tabela_destino"],
    )
    sink.writeFrame(dynamic_frame)


def separar_bucket_prefixo_s3(caminho_s3: str) -> tuple[str, str]:
    """Separa bucket e prefixo de um caminho S3."""
    parsed = urlparse(caminho_s3)
    if not parsed.netloc:
        raise ValueError(f"Caminho S3 invalido: {caminho_s3}")
    return parsed.netloc, parsed.path.lstrip("/")


def gravar_csv(glue_context: Any, df: Any, destino: dict[str, Any]) -> None:
    """Grava CSV em S3 com nome final configurado."""
    import boto3

    pasta_destino = f"{destino['s3_destino'].rstrip('/')}/{destino['nome_pasta_destino']}/"
    caminho_tmp = f"{pasta_destino}_tmp/"
    caminho_final = f"{pasta_destino}{destino['nome_arquivo_csv']}"

    glue_context.purge_s3_path(caminho_tmp, {"retentionPeriod": 0})
    df.coalesce(1).write.mode("overwrite").option(
        "header", destino.get("incluir_cabecalho", True)
    ).csv(caminho_tmp)

    s3_client = boto3.client("s3")
    bucket_tmp, prefixo_tmp = separar_bucket_prefixo_s3(caminho_tmp)
    bucket_final, chave_final = separar_bucket_prefixo_s3(caminho_final)
    resposta = s3_client.list_objects_v2(Bucket=bucket_tmp, Prefix=prefixo_tmp)
    parts = [
        item["Key"]
        for item in resposta.get("Contents", [])
        if item["Key"].split("/")[-1].startswith("part-")
    ]
    if len(parts) != 1:
        raise ValueError("Esperado exatamente um arquivo part-* para CSV.")

    s3_client.delete_object(Bucket=bucket_final, Key=chave_final)
    s3_client.copy_object(
        Bucket=bucket_final,
        Key=chave_final,
        CopySource={"Bucket": bucket_tmp, "Key": parts[0]},
    )
    glue_context.purge_s3_path(caminho_tmp, {"retentionPeriod": 0})


def gravar_resultado(glue_context: Any, df: Any, destino: dict[str, Any]) -> None:
    """Grava resultado conforme tipo de destino."""
    tipo_saida = destino["tipo_saida_destino"]
    if tipo_saida == "catalogo":
        gravar_catalogo(glue_context, df, destino)
        return
    if tipo_saida == "csv":
        gravar_csv(glue_context, df, destino)
        return
    raise ValueError("tipo_saida_destino invalido. Use 'catalogo' ou 'csv'.")
