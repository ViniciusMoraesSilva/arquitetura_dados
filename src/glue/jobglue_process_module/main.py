"""Job Glue principal para leitura, processamento SQL e gravação de resultados."""

import json
import logging
import sys
from datetime import datetime
from string import Formatter
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.session import SparkSession

try:
    import boto3
    from src.shared.control_plane import DynamoDBControlPlane
except ImportError:  # pragma: no cover - Glue runtime fallback
    import boto3
    DynamoDBControlPlane = None

try:
    from .utils import executar_consultas_sql, gravar_catalogo_glue
    from .utils.config_origem_dados import obter_configuracao_leitura_origem
except ImportError:  # pragma: no cover - Glue script execution fallback
    from utils import executar_consultas_sql, gravar_catalogo_glue
    from utils.config_origem_dados import obter_configuracao_leitura_origem

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
CONFIG_ORIGEM_DADOS_PATH = (
    Path(__file__).resolve().parent / "config" / "config_origem_dados.json"
)
CONFIG_DESTINO_DADOS_PATH = (
    Path(__file__).resolve().parent / "config" / "config_destino_dados.json"
)


def _read_text_from_local_or_s3(path: str) -> str:
    if path.startswith("s3://"):
        parsed = urlparse(path)
        response = boto3.client("s3", region_name=AWS_REGION).get_object(
            Bucket=parsed.netloc,
            Key=parsed.path.lstrip("/"),
        )
        return response["Body"].read().decode("utf-8")
    return Path(path).read_text(encoding="utf-8")


def _join_relative_to_base(base_path: str, relative_path: str) -> str:
    if relative_path.startswith("s3://") or Path(relative_path).is_absolute():
        return relative_path
    if base_path.startswith("s3://"):
        parsed = urlparse(base_path)
        base_prefix = parsed.path.rsplit("/", 1)[0].strip("/")
        key = f"{base_prefix}/{relative_path}" if base_prefix else relative_path
        return f"s3://{parsed.netloc}/{key}"
    return str(Path(base_path).resolve().parent / relative_path)


class JobGlue:
    """Orquestra o fluxo completo do job Glue."""

    def __init__(self) -> None:
        self.args = self.resolver_argumentos_job()
        self.control_plane = self.criar_control_plane()
        self.ingestion_metadata = self.carregar_metadata_ingestao()
        self.sc, self.glue_context, self.spark, self.job, self.glue_client = (
            self.inicializar_o_contexto()
        )

    def resolver_argumentos_job(self) -> dict[str, str]:
        """
        Resolve os argumentos obrigatórios recebidos pelo job.

        Returns:
            Dicionário com os argumentos obrigatórios da execução.
        """
        modo = getResolvedOptions(sys.argv, ["JOB_MODE"])["JOB_MODE"].lower()
        argumentos_base = [
            "JOB_NAME",
            "JOB_MODE",
            "GLUE_SQL_LOCATION",
            "AMBIENTE",
            "CONTROL_TABLE_NAME",
        ]
        if modo == "sor":
            argumentos = getResolvedOptions(sys.argv, argumentos_base + ["INGESTION_ID"])
            argumentos["JOB_MODE"] = modo
            return argumentos
        if modo == "process":
            argumentos = getResolvedOptions(sys.argv, argumentos_base + ["PROCESS_ID", "MODULE_CONFIG"])
            argumentos["JOB_MODE"] = modo
            return argumentos
        raise ValueError("JOB_MODE invalido. Use 'sor' ou 'process'.")

    def criar_control_plane(self):
        """Cria o adaptador de plano de controle usado pelo contrato MVP1."""
        if DynamoDBControlPlane is None:
            raise RuntimeError("DynamoDBControlPlane nao esta disponivel no runtime.")
        return DynamoDBControlPlane(self.args["CONTROL_TABLE_NAME"])

    def carregar_metadata_ingestao(self) -> dict[str, Any]:
        """Carrega metadata do INGESTION_ID somente no modo SOR."""
        if self.args["JOB_MODE"] != "sor":
            return {}
        return self.control_plane.get_ingestion_metadata(self.args["INGESTION_ID"])

    def inicializar_o_contexto(
        self,
    ) -> tuple[SparkContext, GlueContext, SparkSession, Job, Any]:
        """
        Inicializa os contextos necessários do Spark e do Glue.

        Returns:
            Tupla contendo ``SparkContext``, ``GlueContext``, ``SparkSession``,
            instância de ``Job`` e client boto3 do Glue.
        """
        sc = SparkContext()
        glue_context = GlueContext(sc)
        spark = glue_context.spark_session
        job = Job(glue_context)
        glue_client = boto3.client("glue", region_name=AWS_REGION)
        return sc, glue_context, spark, job, glue_client

    def inicializar_rastreio_job(self) -> None:
        """
        Inicializa o rastreio da execução do job no Glue.
        """
        try:
            self.job.init(self.args["JOB_NAME"], self.args)
        except Exception:
            logger.exception("[INICIALIZAR_RASTREIO_JOB] Erro ao inicializar job")
            raise

    def resolver_predicate_tabela(self, table: str, partition_padrao: str) -> str:
        """
        Resolve o predicate final de leitura para uma tabela.

        No modo process, a SOR é lida exclusivamente pelo ingestion_id travado
        em ``PROCESS#<process_id> / INPUT_LOCK#<table>``. No modo SOR, o
        predicate declarativo da metadata/configuração é preservado.

        Args:
            table: Nome lógico da tabela.
            partition_padrao: Predicate padrão da chamada.

        Returns:
            Predicate final a ser usado na leitura.
        """
        if self.args["JOB_MODE"] == "process":
            if partition_padrao:
                logger.info(
                    "Utilizando predicate declarativo rastreavel para %s: %s",
                    table,
                    partition_padrao,
                )
                return partition_padrao
            lock = self.control_plane.require_input_lock(self.args["PROCESS_ID"], table)
            ingestion_id = lock["ingestion_id"]
            predicate = f"ingestion_id='{ingestion_id}'"
            logger.info("Predicate SOR resolvido via INPUT_LOCK para %s: %s", table, predicate)
            return predicate

        logger.info(
            "Utilizando predicate declarativo para a tabela %s: %s",
            table,
            partition_padrao,
        )

        return partition_padrao

    def carregar_dados_glue(
        self,
        tipo_origem: str,
        table: str,
        description: str,
        database: str = None,
        partition: str = None,
        use_catalog: bool = True,
        caminho_s3: str = None,
        header: bool = True,
        delimiter: str = ",",
        infer_schema: bool = True,
        encoding: str = "UTF-8",
        quote: str = '"',
        escape: str = "\\",
    ) -> DataFrame:
        """
        Carrega dados em ``DataFrame`` a partir do Glue Catalog ou de CSV no S3.

        Args:
            tipo_origem: Tipo da origem, como ``catalogo`` ou ``csv``.
            table: Nome lógico da tabela/view no fluxo.
            description: Descrição amigável usada nos logs.
            database: Database do Glue Catalog quando a origem for ``catalogo``.
            partition: Predicate base da leitura.
            use_catalog: Indica se o schema do catálogo deve ser usado na leitura.
            caminho_s3: Caminho do arquivo CSV no S3 quando a origem for ``csv``.
            header: Indica se o CSV possui cabeçalho.
            delimiter: Delimitador do arquivo CSV.
            infer_schema: Define se o schema do CSV será inferido.
            encoding: Encoding do arquivo CSV.
            quote: Caractere de aspas do CSV.
            escape: Caractere de escape do CSV.

        Returns:
            DataFrame carregado da origem configurada.
        """
        predicate = self.resolver_predicate_tabela(table, partition)
        configuracao_leitura_origem = obter_configuracao_leitura_origem()
        group_files = configuracao_leitura_origem["group_files"]
        group_size_bytes = str(configuracao_leitura_origem["group_size_bytes"])

        if tipo_origem == "catalogo":
            if not database:
                raise ValueError(
                    "O parametro 'database' e obrigatorio para tipo_origem='catalogo'."
                )

            logger.info(
                "PARAMETROS UTILIZADOS NA LEITURA DO CATALOGO %s, %s, %s, %s",
                description,
                database,
                table,
                predicate,
            )

            parametros_leitura = {
                "database": f"{database}",
                "table_name": f"{table}",
                "useCatalogSchema": use_catalog,
                "additional_options": {
                    "groupFiles": group_files,
                    "groupSize": group_size_bytes,
                },
            }

            if predicate:
                parametros_leitura["push_down_predicate"] = predicate

            df = self.glue_context.create_data_frame.from_catalog(**parametros_leitura)
        elif tipo_origem == "csv":
            if not caminho_s3:
                raise ValueError(
                    "O parametro 'caminho_s3' e obrigatorio para tipo_origem='csv'."
                )

            logger.info(
                "PARAMETROS UTILIZADOS NA LEITURA DO CSV %s, %s, header=%s, delimiter=%s, infer_schema=%s",
                description,
                caminho_s3,
                header,
                delimiter,
                infer_schema,
            )

            dynamic_frame_csv = self.glue_context.create_dynamic_frame.from_options(
                connection_type="s3",
                connection_options={
                    "paths": [caminho_s3],
                    "groupFiles": group_files,
                    "groupSize": group_size_bytes,
                },
                format="csv",
                format_options={
                    "withHeader": header,
                    "separator": delimiter,
                    "quoteChar": quote,
                    "escaper": escape,
                    "inferSchema": infer_schema,
                    "encoding": encoding,
                },
            )
            df = dynamic_frame_csv.toDF()

            if predicate:
                logger.info(
                    "Aplicando filtro no CSV %s com o predicate: %s",
                    description,
                    predicate,
                )
                df = df.filter(predicate)
        else:
            raise ValueError("tipo_origem invalido. Use 'catalogo' ou 'csv'.")

        return df

    def carregar_catalogo(
        self,
        table: str,
        description: str,
        tipo_origem: str = "catalogo",
        database: str = None,
        partition: str = None,
        caminho_s3: str = None,
        header: bool = True,
        delimiter: str = ",",
        infer_schema: bool = True,
        encoding: str = "UTF-8",
        quote: str = '"',
        escape: str = "\\",
        campos_duplicidade: list = None,
        use_catalog: bool = True,
    ) -> DataFrame:
        """
        Carrega uma fonte de dados, aplica deduplicação opcional e registra temp view.

        Args:
            table: Nome da temp view a ser registrada.
            description: Descrição amigável usada em log.
            tipo_origem: Tipo da origem de dados.
            database: Database do catálogo quando aplicável.
            partition: Predicate base da leitura.
            caminho_s3: Caminho do CSV no S3 quando aplicável.
            header: Indica se o CSV possui cabeçalho.
            delimiter: Delimitador do CSV.
            infer_schema: Define se o schema do CSV será inferido.
            encoding: Encoding do CSV.
            quote: Caractere de aspas do CSV.
            escape: Caractere de escape do CSV.
            campos_duplicidade: Colunas usadas na deduplicação.
            use_catalog: Indica se o schema do catálogo deve ser usado.

        Returns:
            DataFrame final carregado e registrado como temp view.
        """
        try:
            db_local = self.args.get("DBLOCAL", "false")
            if self.args["AMBIENTE"] != "prd" and db_local.lower() == "true":
                logger.info("Carregando ambiente de teste, utilizando bases mockadas.")

            database_origem = (
                "dblocal"
                if tipo_origem == "catalogo"
                and self.args["AMBIENTE"] != "prd"
                and db_local.lower() == "true"
                else database
            )

            df = self.carregar_dados_glue(
                tipo_origem=tipo_origem,
                database=database_origem,
                table=table,
                partition=partition,
                description=description,
                use_catalog=use_catalog,
                caminho_s3=caminho_s3,
                header=header,
                delimiter=delimiter,
                infer_schema=infer_schema,
                encoding=encoding,
                quote=quote,
                escape=escape,
            )

            if campos_duplicidade is not None:
                if campos_duplicidade == ["*"]:
                    df = df.dropDuplicates()
                else:
                    df = df.dropDuplicates(campos_duplicidade)
                logger.info("Duplicidades removidas da fonte %s", description)

            df.createOrReplaceTempView(table)

            return df

        except Exception:
            logger.exception("[CARREGAR_FONTES_DADOS] Erro ao carregar fonte")
            raise

    def carregar_config_origem_dados(self) -> dict[str, Any]:
        """
        Carrega a configuração declarativa das origens de dados.

        Returns:
            Configuração completa das origens de dados no JSON.
        """
        module_config = self.carregar_module_config()
        if "config_origem_dados" in module_config:
            return module_config["config_origem_dados"]

        if not CONFIG_ORIGEM_DADOS_PATH.exists():
            raise FileNotFoundError(
                "Arquivo de configuracao das origens nao encontrado: "
                f"{CONFIG_ORIGEM_DADOS_PATH}"
            )

        with CONFIG_ORIGEM_DADOS_PATH.open(encoding="utf-8") as arquivo_config:
            return json.load(arquivo_config)

    def carregar_module_config(self) -> dict[str, Any]:
        """Carrega MODULE_CONFIG quando o job roda em modo process."""
        if self.args.get("JOB_MODE") != "process":
            return {}
        if hasattr(self, "_module_config"):
            return self._module_config

        module_config_path = self.args["MODULE_CONFIG"]
        self._module_config = json.loads(_read_text_from_local_or_s3(module_config_path))
        self._module_config_base = module_config_path
        return self._module_config

    def resolver_caminho_sql(self) -> str:
        module_config = self.carregar_module_config()
        sql_file = module_config.get("sql_file")
        if sql_file:
            return _join_relative_to_base(self.args["MODULE_CONFIG"], sql_file)
        return self.args["GLUE_SQL_LOCATION"] + "/src/sql/consultas.sql"

    def listar_origens_configuradas(self) -> list[dict[str, Any]]:
        """
        Consolida as origens configuradas dos blocos ``catalogo`` e ``csv``.

        Returns:
            Lista de origens com seu respectivo bloco de origem.
        """
        configuracao_origens = self.carregar_config_origem_dados()
        configuracao_catalogo = configuracao_origens.get("catalogo", {})
        configuracao_csv = configuracao_origens.get("csv", {})
        origens_catalogo = configuracao_catalogo.get("fontes", [])
        origens_csv = configuracao_csv.get("fontes", [])

        return [
            {"bloco_origem": "catalogo", **origem}
            for origem in origens_catalogo
        ] + [
            {"bloco_origem": "csv", **origem}
            for origem in origens_csv
        ]

    def montar_contexto_templates_origem(self) -> dict[str, str]:
        """
        Monta o contexto permitido para resolução de placeholders das origens.

        Returns:
            Dicionário com variáveis dinâmicas autorizadas.
        """
        data_ref = (
            self.ingestion_metadata.get("data_referencia")
            or self.args.get("DATA_REF")
            or datetime.now().strftime("%Y%m")
        )

        if len(data_ref) == 6:
            data_ref_datetime = datetime.strptime(data_ref, "%Y%m")
            ano_mes_data_ref = data_ref
        elif len(data_ref) == 8:
            data_ref_datetime = datetime.strptime(data_ref, "%Y%m%d")
            ano_mes_data_ref = data_ref[:6]
        else:
            raise ValueError(
                "Argumento DATA_REF invalido. Use os formatos YYYYMM ou YYYYMMDD."
            )

        primeiro_dia_mes_atual = data_ref_datetime.replace(day=1)
        if primeiro_dia_mes_atual.month == 1:
            ano_mes_anterior = f"{primeiro_dia_mes_atual.year - 1}12"
        else:
            ano_mes_anterior = (
                f"{primeiro_dia_mes_atual.year}"
                f"{primeiro_dia_mes_atual.month - 1:02d}"
            )

        return {
            "DATA_REF": data_ref,
            "DATA_ATUAL": datetime.now().strftime("%Y%m%d"),
            "ANO_DATA_REF": f"{data_ref_datetime.year}",
            "MES_DATA_REF": f"{data_ref_datetime.month:02d}",
            "ANO_MES_DATA_REF": ano_mes_data_ref,
            "MES_ANTERIOR": ano_mes_anterior[-2:],
            "ANO_MES_ANTERIOR": ano_mes_anterior,
            "PROCESS_ID": self.args.get("PROCESS_ID", ""),
            "INGESTION_ID": self.args.get("INGESTION_ID", ""),
        }

    def resolver_template_campo_origem(
        self,
        campo: str,
        valor: Any,
        contexto: dict[str, str],
    ) -> Any:
        """
        Resolve placeholders autorizados em um campo da configuração de origem.

        Args:
            campo: Nome do campo da fonte.
            valor: Valor configurado no JSON.
            contexto: Contexto dinâmico permitido para substituição.

        Returns:
            Valor final resolvido.
        """
        if not isinstance(valor, str):
            return valor

        placeholders = [
            nome_campo
            for _, nome_campo, _, _ in Formatter().parse(valor)
            if nome_campo is not None
        ]

        placeholders_desconhecidos = [
            placeholder
            for placeholder in placeholders
            if placeholder not in contexto
        ]
        if placeholders_desconhecidos:
            raise ValueError(
                f"Placeholder desconhecido na configuracao da origem para o campo {campo}: "
                + ", ".join(placeholders_desconhecidos)
            )

        return valor.format(**contexto)

    def resolver_origem_configurada(
        self,
        origem: dict[str, Any],
        contexto: dict[str, str],
    ) -> dict[str, Any]:
        """
        Resolve dinamicamente os campos textuais de uma origem configurada.

        Args:
            origem: Dicionário bruto lido do JSON.
            contexto: Contexto permitido para substituição.

        Returns:
            Dicionário com valores resolvidos da origem.
        """
        return {
            campo: self.resolver_template_campo_origem(campo, valor, contexto)
            for campo, valor in origem.items()
        }

    def mapear_origem_para_carregar_catalogo(
        self, origem: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Converte a origem declarativa para o contrato interno de ``carregar_catalogo``.

        Args:
            origem: Origem já resolvida dinamicamente.

        Returns:
            Dicionário final compatível com ``carregar_catalogo``.
        """
        bloco_origem = origem["bloco_origem"]

        if bloco_origem == "catalogo":
            return {
                "database": origem["database_origem"],
                "table": origem["tabela_origem"],
                "description": origem["descricao_origem"],
                "partition": origem.get("filtro_origem"),
                "tipo_origem": "catalogo",
            }

        if bloco_origem == "csv":
            return {
                "caminho_s3": origem["caminho_s3_origem"],
                "table": origem["nome_view"],
                "description": origem["descricao_origem"],
                "partition": origem.get("filtro_origem"),
                "tipo_origem": "csv",
                "header": origem.get("header", True),
                "delimiter": origem.get("delimiter", ","),
                "infer_schema": origem.get("infer_schema", True),
                "encoding": origem.get("encoding", "UTF-8"),
                "quote": origem.get("quote", '"'),
                "escape": origem.get("escape", "\\"),
            }

        raise ValueError(
            "Bloco de origem invalido. Use 'catalogo' ou 'csv'."
        )

    def resolver_fonte_processada(self, origem: dict[str, Any]) -> dict[str, Any]:
        """Resolve uma fonte declarativa já considerando o contrato MVP1."""
        fonte = self.mapear_origem_para_carregar_catalogo(origem)
        fonte["partition"] = self.resolver_predicate_tabela(
            fonte["table"],
            fonte.get("partition"),
        )
        return fonte

    def montar_fontes(self) -> list[dict[str, Any]]:
        """
        Monta a lista declarativa de fontes de dados da execução atual.

        Returns:
            Lista de definições de fonte compatíveis com ``carregar_catalogo``.
        """
        contexto = self.montar_contexto_templates_origem()
        origens_configuradas = self.listar_origens_configuradas()

        return [
            self.resolver_fonte_processada(
                self.resolver_origem_configurada(origem, contexto)
            )
            for origem in origens_configuradas
        ]

    def carregar_fontes(self) -> None:
        """
        Carrega as fontes necessárias e registra as temp views do fluxo.
        """
        try:
            for fonte in self.montar_fontes():
                logger.info(
                    "Carregando fonte com os parametros %s",
                    fonte,
                )
                self.carregar_catalogo(**fonte)

        except Exception:
            logger.exception("[CARREGAR_FONTES] Erro ao executar job")
            raise

    def executar_consultas(self) -> DataFrame:
        """
        Executa as consultas SQL do fluxo e retorna o DataFrame final.

        Returns:
            DataFrame final resultante das consultas SQL.
        """
        try:
            pquery = self.resolver_caminho_sql()

            df = executar_consultas_sql(
                self.spark,
                pquery,
                template_context={
                    "PROCESS_ID": self.args.get("PROCESS_ID", ""),
                    "INGESTION_ID": self.args.get("INGESTION_ID", ""),
                },
            ).withColumn(
                "chave_processamento_registro", monotonically_increasing_id()
            )

            return df

        except Exception:
            logger.exception("[EXECUTAR_CONSULTAS] Erro ao executar job")
            raise

    def gravar_resultado(self, df_entrada: DataFrame) -> None:
        """
        Grava o resultado final no destino configurado no Glue Catalog.

        Args:
            df_entrada: DataFrame final a ser persistido.
        """
        try:
            gravar_catalogo_glue(self.glue_context, df_entrada, self.args, logger)
        except Exception:
            logger.exception("[GRAVAR_RESULTADO] Erro ao executar job")
            raise

    def executar_fluxo_de_processamento(self) -> None:
        """Executa o fluxo principal do job em três etapas."""
        self.inicializar_rastreio_job()

        logger.info("===Etapa 1: Carregando fontes===")
        self.carregar_fontes()
        logger.info("****Etapa 1: Fontes carregadas com sucesso****")

        logger.info("===Etapa 2: Executando consultas===")
        df = self.executar_consultas()
        logger.info("****Etapa 2: Consultas executadas com sucesso****")

        logger.info("===Etapa 3: Gravando resultado===")
        self.gravar_resultado(df)
        logger.info("****Etapa 3: Resultado gravado com sucesso****")


if __name__ == "__main__":
    job = JobGlue()
    job.executar_fluxo_de_processamento()
