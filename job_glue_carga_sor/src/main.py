"""Glue Job unico para execucoes SOT e SOR."""

from __future__ import annotations

import logging
import sys
from dataclasses import dataclass
from datetime import datetime
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
from utils.config_destino_dados import (
    obter_configuracao_destino_sor,
    obter_configuracao_destino_sot,
)
from utils.config_origem_dados import (
    obter_configuracao_execucao_sor,
    obter_configuracao_execucao_sot,
    obter_configuracao_leitura_origem,
)
from utils.executar_consultas_sql import executar_consultas_sql
from utils.gravar_sor_glue import gravar_resultado_glue
from utils.metadata_ingestao import obter_metadata_ingestao
from utils.obter_predicate_override_sor_ddb import (
    obter_predicate_override_sor_ddb,
    obter_predicates_override_sot_ddb,
)
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
SQL_BASE_PATH = Path(__file__).resolve().parent
MODO_EXECUCAO_SOT = "sot"
MODO_EXECUCAO_SOR = "sor"


@dataclass(frozen=True)
class ExecutionPlan:
    """Plano interno comum para execucoes SOT e SOR."""

    modo_execucao: str
    chave_execucao: str
    contexto_templates: dict[str, str]
    fontes: list[dict[str, Any]]
    caminho_sql: str
    configuracao_destino: dict[str, Any]
    colunas_tecnicas: dict[str, str]
    metadata: dict[str, Any] | None = None


class JobGlueCargaSor:
    """Orquestra o fluxo unico do Glue para os modos SOT e SOR."""

    def __init__(self) -> None:
        self.args = self.resolver_argumentos_job()
        self.predicates = self.resolver_argumento_predicates()
        self.usar_predicate_override_ddb = self.resolver_flag_predicate_override_ddb()
        self.pipeline_control_table = self.resolver_tabela_pipeline_control()
        self.predicates_override_sot_ddb = self.carregar_predicates_override_sot_ddb()
        self.sc, self.glue_context, self.spark, self.job = self.inicializar_contexto()
        self.dynamodb_resource = boto3.resource("dynamodb", region_name=AWS_REGION)

    def resolver_argumentos_job(self) -> dict[str, str]:
        """Resolve os argumentos obrigatorios conforme o modo de execucao."""
        argumentos = getResolvedOptions(
            sys.argv,
            [
                "JOB_NAME",
                "MODO_EXECUCAO",
                "AMBIENTE",
                "DBLOCAL",
            ],
        )
        modo_execucao = argumentos["MODO_EXECUCAO"].strip().lower()
        if modo_execucao not in {MODO_EXECUCAO_SOT, MODO_EXECUCAO_SOR}:
            raise ValueError("MODO_EXECUCAO invalido. Use 'sot' ou 'sor'.")

        if modo_execucao == MODO_EXECUCAO_SOR:
            argumentos.update(getResolvedOptions(sys.argv, ["INGESTION_ID"]))
        else:
            argumentos.update(getResolvedOptions(sys.argv, ["PROCESS_ID"]))
            data_ref = self.resolver_argumento_opcional("DATA_REF")
            if data_ref:
                argumentos["DATA_REF"] = data_ref

        argumentos["MODO_EXECUCAO"] = modo_execucao
        return argumentos

    @staticmethod
    def resolver_argumento_opcional(nome_argumento: str) -> str | None:
        """Resolve um argumento opcional do Glue."""
        try:
            argumentos = getResolvedOptions(sys.argv, [nome_argumento])
            return argumentos[nome_argumento]
        except GlueArgumentError:
            return None

    def resolver_tabela_pipeline_control(self) -> str:
        """Resolve o nome opcional da tabela DynamoDB de controle."""
        valor = self.resolver_argumento_opcional("PIPELINE_CONTROL_TABLE")
        return valor or DEFAULT_PIPELINE_CONTROL_TABLE

    @staticmethod
    def parsear_table_predicates(predicates_str: str) -> dict[str, str]:
        """Parseia predicates opcionais no formato nome_fonte::predicate."""
        resultado = {}

        for item in predicates_str.split(";"):
            item = item.strip()
            if not item:
                continue

            if "::" not in item:
                logger.warning(
                    "Predicate ignorado por formato invalido: %s. Use nome_fonte::predicate",
                    item,
                )
                continue

            nome_fonte, predicate = item.split("::", 1)
            nome_fonte = nome_fonte.strip()
            predicate = predicate.strip()

            if nome_fonte and predicate:
                resultado[nome_fonte] = predicate

        return resultado

    def resolver_argumento_predicates(self) -> dict[str, str] | None:
        """Resolve predicates opcionais por nome de fonte."""
        valor = self.resolver_argumento_opcional("TABLE_PREDICATES")
        if not valor:
            return None

        resultado = self.parsear_table_predicates(valor)
        return resultado if resultado else None

    def resolver_flag_predicate_override_ddb(self) -> bool:
        """Resolve a flag opcional de override de predicate via DynamoDB."""
        valor = self.resolver_argumento_opcional("USAR_PREDICATE_OVERRIDE_DDB")
        return str(valor).strip().lower() == "true"

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

    @staticmethod
    def montar_contexto_templates_sor(metadata: dict[str, Any]) -> dict[str, str]:
        """Monta o contexto permitido para templates SOR."""
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

    @staticmethod
    def montar_contexto_templates_sot(args: dict[str, str]) -> dict[str, str]:
        """Monta o contexto permitido para templates SOT."""
        data_atual = datetime.now().strftime("%Y%m%d")
        data_ref = args.get("DATA_REF", data_atual)

        contexto = {
            "PROCESS_ID": args["PROCESS_ID"],
            "DATA_REF": data_ref,
            "DATA_ATUAL": data_atual,
        }

        if len(data_ref) == 6:
            data_ref_datetime = datetime.strptime(data_ref, "%Y%m")
            ano_mes_data_ref = data_ref
        elif len(data_ref) == 8:
            data_ref_datetime = datetime.strptime(data_ref, "%Y%m%d")
            ano_mes_data_ref = data_ref[:6]
        else:
            raise ValueError(
                "Argumento DATA_REF invalido para SOT. Use os formatos YYYYMM ou YYYYMMDD."
            )

        primeiro_dia_mes_atual = data_ref_datetime.replace(day=1)
        if primeiro_dia_mes_atual.month == 1:
            ano_mes_anterior = f"{primeiro_dia_mes_atual.year - 1}12"
        else:
            ano_mes_anterior = (
                f"{primeiro_dia_mes_atual.year}"
                f"{primeiro_dia_mes_atual.month - 1:02d}"
            )

        contexto.update(
            {
                "ANO_DATA_REF": f"{data_ref_datetime.year}",
                "MES_DATA_REF": f"{data_ref_datetime.month:02d}",
                "ANO_MES_DATA_REF": ano_mes_data_ref,
                "MES_ANTERIOR": ano_mes_anterior[-2:],
                "ANO_MES_ANTERIOR": ano_mes_anterior,
            }
        )
        return contexto

    def montar_contexto_templates_origem(
        self,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, str]:
        """Compatibilidade com o nome antigo do metodo SOR."""
        metadata_final = self if metadata is None else metadata
        return JobGlueCargaSor.montar_contexto_templates_sor(metadata_final)

    def carregar_predicates_override_sot_ddb(self) -> dict[str, str]:
        """Carrega predicates override SOT quando o modo e a flag permitirem."""
        if (
            self.args.get("MODO_EXECUCAO") != MODO_EXECUCAO_SOT
            or not self.usar_predicate_override_ddb
        ):
            return {}

        try:
            dynamodb_resource = boto3.resource("dynamodb", region_name=AWS_REGION)
            return obter_predicates_override_sot_ddb(
                dynamodb_resource=dynamodb_resource,
                job_name=self.args["JOB_NAME"],
                process_id=self.args["PROCESS_ID"],
            )
        except Exception as ex:
            logger.warning(
                "Falha ao consultar override de predicate SOT no DynamoDB para process_id=%s. detalhe=%s",
                self.args["PROCESS_ID"],
                ex,
            )
            return {}

    def resolver_predicate_tabela(
        self,
        metadata: dict[str, Any],
        predicate_padrao: str | None,
    ) -> str | None:
        """Compatibilidade com a resolucao antiga de predicate SOR."""
        source_table_name = metadata["source_table_name"]

        if self.predicates is not None and source_table_name in self.predicates:
            predicate = self.predicates[source_table_name]
            logger.warning(
                "Utilizando predicate override de TABLE_PREDICATES para %s: %s",
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
                    "Utilizando predicate override SOR do DynamoDB para %s: %s",
                    source_table_name,
                    predicate_override_ddb,
                )
                return predicate_override_ddb

        logger.info(
            "Nenhum override encontrado para %s. Utilizando predicate padrao: %s",
            source_table_name,
            predicate_padrao,
        )
        return predicate_padrao

    def resolver_predicate_fonte(
        self,
        fonte: dict[str, Any],
        predicate_padrao: str | None,
        metadata: dict[str, Any] | None = None,
    ) -> str | None:
        """Resolve predicate final com prioridade argumento, DynamoDB e config."""
        nome_fonte = self.obter_chave_predicate_fonte(fonte)

        if self.predicates is not None and nome_fonte in self.predicates:
            predicate = self.predicates[nome_fonte]
            logger.warning(
                "Utilizando predicate override de TABLE_PREDICATES para %s: %s",
                nome_fonte,
                predicate,
            )
            return predicate

        if fonte.get("modo_execucao") == MODO_EXECUCAO_SOR and self.usar_predicate_override_ddb:
            if metadata is None:
                raise ValueError("Metadata e obrigatoria para predicate override SOR.")

            predicate_override_ddb = obter_predicate_override_sor_ddb(
                dynamodb_resource=self.dynamodb_resource,
                metadata=metadata,
            )
            if predicate_override_ddb:
                logger.warning(
                    "Utilizando predicate override SOR do DynamoDB para %s: %s",
                    nome_fonte,
                    predicate_override_ddb,
                )
                return predicate_override_ddb

        if fonte.get("modo_execucao") == MODO_EXECUCAO_SOT:
            predicate_override_sot = self.predicates_override_sot_ddb.get(nome_fonte)
            if predicate_override_sot:
                logger.warning(
                    "Utilizando predicate override SOT do DynamoDB para %s: %s",
                    nome_fonte,
                    predicate_override_sot,
                )
                return predicate_override_sot

        logger.info(
            "Nenhum override encontrado para %s. Utilizando predicate padrao: %s",
            nome_fonte,
            predicate_padrao,
        )
        return predicate_padrao

    @staticmethod
    def obter_chave_predicate_fonte(fonte: dict[str, Any]) -> str:
        """Resolve o nome usado para overrides de predicate."""
        return str(
            fonte.get("source_table_name")
            or fonte.get("tabela_origem")
            or fonte.get("nome_view")
        )

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
            "[+] Total de linhas lidos da origem %s DEPOIS DA REMOCAO DE DUPLICIDADE: %s",
            configuracao_origem.get(
                "descricao_origem",
                configuracao_origem.get("source_table_name")
                or configuracao_origem.get("tabela_origem")
                or configuracao_origem.get("nome_view"),
            ),
            df.count(),
        )
        return df

    def carregar_fonte(
        self,
        fonte: dict[str, Any],
        metadata: dict[str, Any] | None = None,
    ) -> DataFrame:
        """Le uma fonte configurada e registra sua temp view."""
        configuracao_leitura = obter_configuracao_leitura_origem()
        tipo_origem = fonte.get("tipo_origem", "catalogo").strip().lower()
        nome_view = fonte.get("nome_view") or fonte.get("source_table_name") or fonte.get("tabela_origem")
        predicate = self.resolver_predicate_fonte(
            fonte,
            fonte.get("filtro_origem"),
            metadata,
        )

        if tipo_origem == "catalogo":
            database_origem = (
                "dblocal"
                if self.args["AMBIENTE"] != "prd"
                and self.args["DBLOCAL"].lower() == "true"
                else fonte["database_origem"]
            )
            tabela_origem = fonte["tabela_origem"]
            parametros_leitura = {
                "database": database_origem,
                "table_name": tabela_origem,
                "useCatalogSchema": fonte.get("use_catalog", True),
                "additional_options": {
                    "groupFiles": configuracao_leitura["group_files"],
                    "groupSize": str(configuracao_leitura["group_size_bytes"]),
                },
            }
            if predicate:
                parametros_leitura["push_down_predicate"] = predicate

            logger.info(
                "Lendo fonte catalogo database=%s table=%s view=%s predicate=%s",
                database_origem,
                tabela_origem,
                nome_view,
                predicate,
            )
            df = self.glue_context.create_data_frame.from_catalog(**parametros_leitura)
        elif tipo_origem == "csv":
            caminho_s3 = fonte["caminho_s3_origem"]
            logger.info(
                "Lendo fonte CSV path=%s view=%s predicate=%s",
                caminho_s3,
                nome_view,
                predicate,
            )
            dynamic_frame_csv = self.glue_context.create_dynamic_frame.from_options(
                connection_type="s3",
                connection_options={
                    "paths": [caminho_s3],
                    "groupFiles": configuracao_leitura["group_files"],
                    "groupSize": str(configuracao_leitura["group_size_bytes"]),
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
            df = dynamic_frame_csv.toDF()
            if predicate:
                df = df.filter(predicate)
        else:
            raise ValueError("tipo_origem invalido. Use 'catalogo' ou 'csv'.")

        df = self.aplicar_deduplicacao_origem(df, fonte)
        df.createOrReplaceTempView(nome_view)
        logger.info(
            "Fonte carregada e registrada como temp view %s. Linhas=%s",
            nome_view,
            df.count(),
        )
        return df

    def carregar_fontes(self, plano: ExecutionPlan) -> None:
        """Carrega todas as fontes da execucao."""
        for fonte in plano.fontes:
            self.carregar_fonte(fonte, plano.metadata)

    @staticmethod
    def resolver_caminho_sql_configurado(caminho_sql: str) -> str:
        """Resolve caminho SQL absoluto a partir de path absoluto ou relativo a src."""
        caminho = Path(caminho_sql)
        if not caminho.is_absolute():
            caminho = SQL_BASE_PATH / caminho

        if not caminho.exists():
            raise FileNotFoundError(f"Arquivo SQL da execucao nao encontrado: {caminho}")

        return str(caminho)

    def executar_sql_layout_saida(self, plano: ExecutionPlan) -> DataFrame:
        """Executa o SQL configurado que define o layout final."""
        logger.info("Executando SQL da execucao: %s", plano.caminho_sql)
        return executar_consultas_sql(self.spark, plano.caminho_sql)

    @staticmethod
    def aplicar_colunas_tecnicas(
        df_entrada: DataFrame,
        colunas_tecnicas: dict[str, str],
    ) -> DataFrame:
        """Adiciona colunas tecnicas constantes ao DataFrame final."""
        df_saida = df_entrada
        for coluna, valor in colunas_tecnicas.items():
            df_saida = df_saida.withColumn(coluna, lit(valor))
        return df_saida

    def montar_plano_sor(self) -> ExecutionPlan:
        """Monta o plano de execucao SOR a partir da metadata DynamoDB."""
        metadata = self.carregar_metadata_ingestao()
        contexto = self.montar_contexto_templates_sor(metadata)
        execucao = obter_configuracao_execucao_sor(metadata, contexto)
        caminho_sql = self.resolver_caminho_sql_configurado(execucao["sql_path"])
        configuracao_destino = obter_configuracao_destino_sor(metadata, self.args)

        return ExecutionPlan(
            modo_execucao=MODO_EXECUCAO_SOR,
            chave_execucao=str(metadata["ingestion_id"]),
            contexto_templates=contexto,
            fontes=execucao["fontes"],
            caminho_sql=caminho_sql,
            configuracao_destino=configuracao_destino,
            colunas_tecnicas=execucao.get(
                "post_columns",
                {"ingestion_id": str(metadata["ingestion_id"])},
            ),
            metadata=metadata,
        )

    def montar_plano_sot(self) -> ExecutionPlan:
        """Monta o plano de execucao SOT a partir do PROCESS_ID."""
        process_id = self.args["PROCESS_ID"]
        contexto = self.montar_contexto_templates_sot(self.args)
        execucao = obter_configuracao_execucao_sot(process_id, contexto)
        caminho_sql = self.resolver_caminho_sql_configurado(execucao["sql_path"])
        configuracao_destino = obter_configuracao_destino_sot(process_id, self.args)

        return ExecutionPlan(
            modo_execucao=MODO_EXECUCAO_SOT,
            chave_execucao=process_id,
            contexto_templates=contexto,
            fontes=execucao["fontes"],
            caminho_sql=caminho_sql,
            configuracao_destino=configuracao_destino,
            colunas_tecnicas=execucao.get("post_columns", {"process_id": process_id}),
        )

    def montar_plano_execucao(self) -> ExecutionPlan:
        """Monta o plano de execucao conforme MODO_EXECUCAO."""
        if self.args["MODO_EXECUCAO"] == MODO_EXECUCAO_SOR:
            return self.montar_plano_sor()

        if self.args["MODO_EXECUCAO"] == MODO_EXECUCAO_SOT:
            return self.montar_plano_sot()

        raise ValueError("MODO_EXECUCAO invalido. Use 'sot' ou 'sor'.")

    def gravar_resultado(self, plano: ExecutionPlan, df_entrada: DataFrame) -> None:
        """Aplica colunas tecnicas e grava o resultado final."""
        df_saida = self.aplicar_colunas_tecnicas(df_entrada, plano.colunas_tecnicas)
        gravar_resultado_glue(
            self.glue_context,
            df_saida,
            plano.configuracao_destino,
            logger,
        )

    def executar_fluxo_de_processamento(self) -> None:
        """Executa o fluxo principal do job unico."""
        self.inicializar_rastreio_job()

        logger.info("===Etapa 1: Montando plano de execucao===")
        plano = self.montar_plano_execucao()
        logger.info(
            "****Etapa 1: Plano montado com sucesso modo=%s chave=%s****",
            plano.modo_execucao,
            plano.chave_execucao,
        )

        logger.info("===Etapa 2: Carregando fontes===")
        self.carregar_fontes(plano)
        logger.info("****Etapa 2: Fontes carregadas com sucesso****")

        logger.info("===Etapa 3: Executando SQL===")
        df = self.executar_sql_layout_saida(plano)
        logger.info("****Etapa 3: SQL executado com sucesso****")

        logger.info("===Etapa 4: Gravando resultado===")
        self.gravar_resultado(plano, df)
        logger.info("****Etapa 4: Resultado gravado com sucesso****")


if __name__ == "__main__":
    job = JobGlueCargaSor()
    job.executar_fluxo_de_processamento()
