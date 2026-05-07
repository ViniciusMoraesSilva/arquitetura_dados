"""Glue Job generico para carga de tabelas na camada SOR."""

import logging
import sys
from pathlib import Path
from typing import Any

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import GlueArgumentError, getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from pyspark.sql.session import SparkSession
from utils.config_destino_dados import obter_configuracao_destino
from utils.config_origem_dados import (
    obter_configuracao_leitura_origem,
    obter_configuracao_origem,
)
from utils.executar_consultas_sql import executar_consultas_sql
from utils.gravar_sor_glue import gravar_sor_glue
from utils.metadata_ingestao import obter_metadata_ingestao
from utils.obter_predicate_override_sor_ddb import obter_predicate_override_sor_ddb
from utils.templates import normalizar_nome_placeholder

MSG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
logging.basicConfig(
    level=logging.INFO,
    format=MSG_FORMAT,
    datefmt=DATETIME_FORMAT,
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

AWS_REGION = "us-east-2"
DEFAULT_PIPELINE_CONTROL_TABLE = "pipeline_control"
SQL_BASE_PATH = Path(__file__).resolve().parent / "sql"


class JobGlueCargaSor:
    """Orquestra a carga SOR a partir de metadata, config JSON e SQL."""

    def __init__(self) -> None:
        self.args = self.resolver_argumentos_job()
        self.predicates = self.resolver_argumento_predicates()
        self.usar_predicate_override_ddb = self.resolver_flag_predicate_override_ddb()
        self.pipeline_control_table = self.resolver_tabela_pipeline_control()
        self.sc, self.glue_context, self.spark, self.job = self.inicializar_contexto()
        self.dynamodb_resource = boto3.resource("dynamodb", region_name=AWS_REGION)

    def resolver_argumentos_job(self) -> dict[str, str]:
        """Resolve os argumentos obrigatorios recebidos pelo Glue."""
        return getResolvedOptions(
            sys.argv,
            [
                "JOB_NAME",
                "INGESTION_ID",
                "AMBIENTE",
                "DBLOCAL",
            ],
        )

    def resolver_tabela_pipeline_control(self) -> str:
        """Resolve o nome opcional da tabela DynamoDB de controle."""
        try:
            argumentos = getResolvedOptions(sys.argv, ["PIPELINE_CONTROL_TABLE"])
            return argumentos["PIPELINE_CONTROL_TABLE"]
        except GlueArgumentError:
            return DEFAULT_PIPELINE_CONTROL_TABLE

    @staticmethod
    def parsear_table_predicates(predicates_str: str) -> dict[str, str]:
        """Parseia predicates opcionais no formato source_table_name::predicate."""
        resultado = {}

        for item in predicates_str.split(";"):
            item = item.strip()
            if not item:
                continue

            if "::" not in item:
                logger.warning(
                    "Predicate ignorado por formato invalido: %s. Use source_table_name::predicate",
                    item,
                )
                continue

            source_table_name, predicate = item.split("::", 1)
            source_table_name = source_table_name.strip()
            predicate = predicate.strip()

            if source_table_name and predicate:
                resultado[source_table_name] = predicate

        return resultado

    def resolver_argumento_predicates(self) -> dict[str, str] | None:
        """Resolve predicates opcionais por source_table_name."""
        try:
            predicates = getResolvedOptions(sys.argv, ["TABLE_PREDICATES"])
            resultado = self.parsear_table_predicates(predicates["TABLE_PREDICATES"])
            return resultado if resultado else None
        except GlueArgumentError:
            return None

    def resolver_flag_predicate_override_ddb(self) -> bool:
        """Resolve a flag opcional de override de predicate via DynamoDB."""
        try:
            argumentos = getResolvedOptions(sys.argv, ["USAR_PREDICATE_OVERRIDE_DDB"])
            valor = argumentos["USAR_PREDICATE_OVERRIDE_DDB"]
            return str(valor).strip().lower() == "true"
        except GlueArgumentError:
            return False

    def inicializar_contexto(
        self,
    ) -> tuple[SparkContext, GlueContext, SparkSession, Job]:
        """Inicializa Spark, GlueContext e rastreio do job."""
        sc = SparkContext()
        glue_context = GlueContext(sc)
        spark = glue_context.spark_session
        job = Job(glue_context)
        return sc, glue_context, spark, job

    def inicializar_rastreio_job(self) -> None:
        """Inicializa o rastreio da execucao no Glue."""
        try:
            self.job.init(self.args["JOB_NAME"], self.args)
        except Exception:
            logger.exception("[INICIALIZAR_RASTREIO_JOB] Erro ao inicializar job")
            raise

    def carregar_metadata_ingestao(self) -> dict[str, Any]:
        """Busca a metadata de ingestao no DynamoDB pipeline_control."""
        return obter_metadata_ingestao(
            dynamodb_resource=self.dynamodb_resource,
            table_name=self.pipeline_control_table,
            ingestion_id=self.args["INGESTION_ID"],
        )

    def montar_contexto_templates_origem(
        self,
        metadata: dict[str, Any],
    ) -> dict[str, str]:
        """
        Monta o contexto permitido para resolucao de placeholders da origem.

        Returns:
            Dicionario com variaveis dinamicas autorizadas.
        """
        contexto = {
            "INGESTION_ID": str(metadata["ingestion_id"]),
            "PROCESS_NAME": str(metadata["process_name"]),
            "SOURCE_DATABASE_NAME": str(metadata["source_database_name"]),
            "SOURCE_TABLE_NAME": str(metadata["source_table_name"]),
            "SOR_DATABASE_NAME": str(metadata["sor_database_name"]),
            "SOR_TABLE_NAME": str(metadata["sor_table_name"]),
        }

        for chave, valor in metadata.get("partitions", {}).items():
            nome_placeholder = f"PARTITION_{normalizar_nome_placeholder(str(chave))}"
            contexto[nome_placeholder] = str(valor)

        return contexto

    def resolver_predicate_tabela(
        self,
        metadata: dict[str, Any],
        predicate_padrao: str | None,
    ) -> str | None:
        """Resolve o predicate final da origem SOR."""
        source_table_name = metadata["source_table_name"]

        if self.predicates is not None and source_table_name in self.predicates:
            predicate = self.predicates[source_table_name]
            logger.warning(
                "Utilizando predicate override de TABLE_PREDICATES para a source_table_name %s: %s",
                source_table_name,
                predicate,
            )
            return predicate

        if self.usar_predicate_override_ddb:
            predicate_override_ddb = obter_predicate_override_sor_ddb(
                dynamodb_resource=self.dynamodb_resource,
                metadata=metadata,
            )
            if predicate_override_ddb:
                logger.warning(
                    "Utilizando predicate override do DynamoDB para a source_table_name %s: %s",
                    source_table_name,
                    predicate_override_ddb,
                )
                return predicate_override_ddb

        logger.info(
            "Nenhum override encontrado para a source_table_name %s. Utilizando predicate padrao: %s",
            source_table_name,
            predicate_padrao,
        )
        return predicate_padrao

    def aplicar_deduplicacao_origem(
        self,
        df: DataFrame,
        configuracao_origem: dict[str, Any],
    ) -> DataFrame:
        """Aplica deduplicacao opcional configurada para a origem."""
        campos_duplicidade = configuracao_origem.get("campos_duplicidade")
        if campos_duplicidade is None:
            return df

        if campos_duplicidade == ["*"]:
            df = df.dropDuplicates()
        else:
            df = df.dropDuplicates(campos_duplicidade)

        logger.info(
            "[+] Total de linhas lidos da origem SOR %s DEPOIS DA REMOCAO DE DUPLICIDADE: %s",
            configuracao_origem.get(
                "descricao_origem", configuracao_origem.get("source_table_name")
            ),
            df.count(),
        )
        return df

    def carregar_origem(self, metadata: dict[str, Any]) -> DataFrame:
        """Le a origem configurada e registra as temp views de entrada."""
        contexto = self.montar_contexto_templates_origem(metadata)
        configuracao_origem = obter_configuracao_origem(metadata, contexto)
        configuracao_leitura = obter_configuracao_leitura_origem()
        database_origem = (
            "dblocal"
            if self.args["AMBIENTE"] != "prd" and self.args["DBLOCAL"].lower() == "true"
            else metadata["source_database_name"]
        )
        source_table_name = metadata["source_table_name"]
        predicate = self.resolver_predicate_tabela(
            metadata,
            configuracao_origem.get("filtro_origem"),
        )

        logger.info(
            "Lendo origem SOR database=%s table=%s predicate=%s",
            database_origem,
            source_table_name,
            predicate,
        )

        parametros_leitura = {
            "database": database_origem,
            "table_name": source_table_name,
            "useCatalogSchema": configuracao_origem.get("use_catalog", True),
            "additional_options": {
                "groupFiles": configuracao_leitura["group_files"],
                "groupSize": str(configuracao_leitura["group_size_bytes"]),
            },
        }

        if predicate:
            parametros_leitura["push_down_predicate"] = predicate

        df = self.glue_context.create_data_frame.from_catalog(**parametros_leitura)
        df = self.aplicar_deduplicacao_origem(df, configuracao_origem)
        df.createOrReplaceTempView(source_table_name)

        logger.info(
            "Origem carregada e registrada como temp view %s. Linhas=%s",
            source_table_name,
            df.count(),
        )
        return df

    def resolver_caminho_sql(self, metadata: dict[str, Any]) -> str:
        """Resolve o SQL especifico por process_name e sor_table_name."""
        caminho_sql = (
            SQL_BASE_PATH
            / metadata["process_name"]
            / f"{metadata['sor_table_name']}.sql"
        )
        if not caminho_sql.exists():
            raise FileNotFoundError(
                "Arquivo SQL da carga SOR nao encontrado para "
                f"process_name={metadata['process_name']}, "
                f"sor_table_name={metadata['sor_table_name']}: {caminho_sql}"
            )
        return str(caminho_sql)

    def executar_sql_layout_saida(self, metadata: dict[str, Any]) -> DataFrame:
        """Executa o SQL que define o layout final da tabela SOR."""
        caminho_sql = self.resolver_caminho_sql(metadata)
        logger.info("Executando SQL de layout SOR: %s", caminho_sql)
        return executar_consultas_sql(self.spark, caminho_sql)

    def gravar_resultado(self, metadata: dict[str, Any], df_entrada: DataFrame) -> None:
        """Adiciona ingestion_id e grava o resultado final na SOR."""
        configuracao_destino = obter_configuracao_destino(metadata, self.args)
        df_saida = df_entrada.withColumn("ingestion_id", lit(metadata["ingestion_id"]))
        gravar_sor_glue(self.glue_context, df_saida, configuracao_destino, logger)

    def executar_fluxo_de_processamento(self) -> None:
        """Executa a carga SOR completa."""
        self.inicializar_rastreio_job()

        logger.info("===Etapa 1: Buscando metadata de ingestao===")
        metadata = self.carregar_metadata_ingestao()
        logger.info("****Etapa 1: Metadata carregada com sucesso****")

        logger.info("===Etapa 2: Carregando origem===")
        self.carregar_origem(metadata)
        logger.info("****Etapa 2: Origem carregada com sucesso****")

        logger.info("===Etapa 3: Executando SQL de layout===")
        df = self.executar_sql_layout_saida(metadata)
        logger.info("****Etapa 3: SQL executado com sucesso****")

        logger.info("===Etapa 4: Gravando SOR===")
        self.gravar_resultado(metadata, df)
        logger.info("****Etapa 4: SOR gravada com sucesso****")


if __name__ == "__main__":
    job = JobGlueCargaSor()
    job.executar_fluxo_de_processamento()
