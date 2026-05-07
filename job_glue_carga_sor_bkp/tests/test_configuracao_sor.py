import sys
import types
from pathlib import Path

import pytest

SRC_PATH = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC_PATH))

awsglue_module = types.ModuleType("awsglue")
awsglue_context_module = types.ModuleType("awsglue.context")
awsglue_dynamicframe_module = types.ModuleType("awsglue.dynamicframe")
awsglue_job_module = types.ModuleType("awsglue.job")
awsglue_utils_module = types.ModuleType("awsglue.utils")
pyspark_module = types.ModuleType("pyspark")
pyspark_context_module = types.ModuleType("pyspark.context")
pyspark_sql_module = types.ModuleType("pyspark.sql")
pyspark_sql_functions_module = types.ModuleType("pyspark.sql.functions")
pyspark_sql_session_module = types.ModuleType("pyspark.sql.session")


class GlueArgumentError(Exception):
    pass


awsglue_context_module.GlueContext = object
awsglue_dynamicframe_module.DynamicFrame = object
awsglue_job_module.Job = object
awsglue_utils_module.GlueArgumentError = GlueArgumentError
awsglue_utils_module.getResolvedOptions = lambda *_args, **_kwargs: {}
pyspark_context_module.SparkContext = object
pyspark_sql_module.DataFrame = object
pyspark_sql_module.SparkSession = object
pyspark_sql_functions_module.lit = lambda valor: valor
pyspark_sql_session_module.SparkSession = object

sys.modules.setdefault("awsglue", awsglue_module)
sys.modules.setdefault("awsglue.context", awsglue_context_module)
sys.modules.setdefault("awsglue.dynamicframe", awsglue_dynamicframe_module)
sys.modules.setdefault("awsglue.job", awsglue_job_module)
sys.modules.setdefault("awsglue.utils", awsglue_utils_module)
sys.modules.setdefault("pyspark", pyspark_module)
sys.modules.setdefault("pyspark.context", pyspark_context_module)
sys.modules.setdefault("pyspark.sql", pyspark_sql_module)
sys.modules.setdefault("pyspark.sql.functions", pyspark_sql_functions_module)
sys.modules.setdefault("pyspark.sql.session", pyspark_sql_session_module)

from main import JobGlueCargaSor
from utils.config_destino_dados import obter_configuracao_destino
from utils.config_origem_dados import obter_configuracao_origem
from utils.metadata_ingestao import validar_metadata_ingestao
from utils.obter_predicate_override_sor_ddb import (
    montar_chave_predicate_override_sor,
)
from utils.templates import resolver_template


def metadata_base():
    return {
        "ingestion_id": "ing_100",
        "process_name": "seguros",
        "source_database_name": "db_origem",
        "source_table_name": "contrato",
        "sor_database_name": "sor_db",
        "sor_table_name": "seguros_contratos",
        "partitions": {
            "data_referencia": "202604",
            "dia_base": "15",
        },
        "status": "PROCESSING_SOR",
    }


def test_montar_contexto_template_inclui_partitions():
    contexto = JobGlueCargaSor.montar_contexto_templates_origem(None, metadata_base())

    assert contexto["INGESTION_ID"] == "ing_100"
    assert contexto["PROCESS_NAME"] == "seguros"
    assert contexto["PARTITION_DATA_REFERENCIA"] == "202604"
    assert contexto["PARTITION_DIA_BASE"] == "15"


def test_resolver_template_monta_predicate_de_origem():
    contexto = JobGlueCargaSor.montar_contexto_templates_origem(None, metadata_base())

    predicate = resolver_template(
        "filtro_origem",
        "data_referencia = '{PARTITION_DATA_REFERENCIA}' AND dia_base = '{PARTITION_DIA_BASE}'",
        contexto,
    )

    assert predicate == "data_referencia = '202604' AND dia_base = '15'"


def test_resolver_template_falha_com_placeholder_desconhecido():
    contexto = JobGlueCargaSor.montar_contexto_templates_origem(None, metadata_base())

    with pytest.raises(ValueError, match="PARTITION_INEXISTENTE"):
        resolver_template(
            "filtro_origem",
            "data_referencia = '{PARTITION_INEXISTENTE}'",
            contexto,
        )


def test_validar_metadata_exige_status_processing_sor():
    metadata = metadata_base()
    metadata["status"] = "LOADED_SOR"

    with pytest.raises(ValueError, match="PROCESSING_SOR"):
        validar_metadata_ingestao(metadata, "ing_100")


def test_validar_metadata_exige_partitions_dict_nao_vazio():
    metadata = metadata_base()
    metadata["partitions"] = {}

    with pytest.raises(ValueError, match="partitions"):
        validar_metadata_ingestao(metadata, "ing_100")


def test_obter_configuracao_origem_seleciona_por_metadata_e_resolve_filtro():
    metadata = metadata_base()
    contexto = JobGlueCargaSor.montar_contexto_templates_origem(None, metadata)
    origem = obter_configuracao_origem(metadata, contexto)

    assert origem["process_name"] == "seguros"
    assert origem["source_database_name"] == "db_origem"
    assert origem["source_table_name"] == "contrato"
    assert origem["sor_table_name"] == "seguros_contratos"
    assert origem["filtro_origem"] == "data_referencia = '202604' AND dia_base = '15'"
    assert origem["campos_duplicidade"] == ["num_contrato"]


def test_obter_configuracao_destino_usa_nome_tabela_do_json():
    destino = obter_configuracao_destino(
        metadata_base(),
        {"AMBIENTE": "dev"},
    )

    assert destino["nome_database_destino"] == "sor_db"
    assert destino["nome_tabela_destino"] == "seguros_contratos"
    assert destino["particao_tabela_destino"] == ["ingestion_id"]
    assert destino["s3_destino"] == "s3://bucket-dev/sor"


def test_parsear_table_predicates():
    predicates = JobGlueCargaSor.parsear_table_predicates(
        "contrato::data_referencia = '202604';cliente::id > 10"
    )

    assert predicates == {
        "contrato": "data_referencia = '202604'",
        "cliente": "id > 10",
    }


def test_parsear_table_predicates_ignora_item_mal_formatado():
    predicates = JobGlueCargaSor.parsear_table_predicates(
        "mal_formatado;contrato::data_referencia = '202604'"
    )

    assert predicates == {"contrato": "data_referencia = '202604'"}


def test_montar_chave_predicate_override_sor():
    chave = montar_chave_predicate_override_sor(metadata_base())

    assert chave == {
        "pk": "PROCESS_TABLE#seguros#seguros_contratos",
        "sk": "SOURCE#contrato",
    }


class FakeJob:
    def __init__(
        self,
        predicates=None,
        usar_predicate_override_ddb=False,
        predicate_override_ddb=None,
    ):
        self.predicates = predicates
        self.usar_predicate_override_ddb = usar_predicate_override_ddb
        self.dynamodb_resource = FakeDynamoResource(predicate_override_ddb)


class FakeDynamoResource:
    def __init__(self, predicate):
        self.predicate = predicate

    def Table(self, _table_name):
        return FakeDynamoTable(self.predicate)


class FakeDynamoTable:
    def __init__(self, predicate):
        self.predicate = predicate

    def get_item(self, Key):
        if not self.predicate:
            return {}
        return {
            "Item": {
                **Key,
                "enabled": True,
                "predicate": self.predicate,
            }
        }


def test_resolver_predicate_tabela_argumento_vence_dynamodb_e_config():
    job = FakeJob(
        predicates={"contrato": "predicate_arg"},
        usar_predicate_override_ddb=True,
        predicate_override_ddb="predicate_ddb",
    )

    predicate = JobGlueCargaSor.resolver_predicate_tabela(
        job,
        metadata_base(),
        "predicate_config",
    )

    assert predicate == "predicate_arg"


def test_resolver_predicate_tabela_dynamodb_vence_config():
    job = FakeJob(
        predicates=None,
        usar_predicate_override_ddb=True,
        predicate_override_ddb="predicate_ddb",
    )

    predicate = JobGlueCargaSor.resolver_predicate_tabela(
        job,
        metadata_base(),
        "predicate_config",
    )

    assert predicate == "predicate_ddb"


def test_resolver_predicate_tabela_config_e_fallback():
    job = FakeJob(predicates=None, usar_predicate_override_ddb=False)

    predicate = JobGlueCargaSor.resolver_predicate_tabela(
        job,
        metadata_base(),
        "predicate_config",
    )

    assert predicate == "predicate_config"


class FakeDataFrame:
    def __init__(self):
        self.drop_duplicates_calls = []

    def dropDuplicates(self, columns=None):
        self.drop_duplicates_calls.append(columns)
        return self

    def count(self):
        return 1


def test_aplicar_deduplicacao_origem_sem_config_nao_altera_dataframe():
    df = FakeDataFrame()

    resultado = JobGlueCargaSor.aplicar_deduplicacao_origem(None, df, {})

    assert resultado is df
    assert df.drop_duplicates_calls == []


def test_aplicar_deduplicacao_origem_com_asterisco():
    df = FakeDataFrame()

    resultado = JobGlueCargaSor.aplicar_deduplicacao_origem(
        None,
        df,
        {"campos_duplicidade": ["*"], "descricao_origem": "Contratos"},
    )

    assert resultado is df
    assert df.drop_duplicates_calls == [None]


def test_aplicar_deduplicacao_origem_com_colunas():
    df = FakeDataFrame()

    resultado = JobGlueCargaSor.aplicar_deduplicacao_origem(
        None,
        df,
        {"campos_duplicidade": ["num_contrato"], "descricao_origem": "Contratos"},
    )

    assert resultado is df
    assert df.drop_duplicates_calls == [["num_contrato"]]
