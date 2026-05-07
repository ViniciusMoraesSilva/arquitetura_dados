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

import main
from main import JobGlueCargaSor
from utils.config_destino_dados import (
    obter_configuracao_destino,
    obter_configuracao_destino_sot,
)
from utils.config_origem_dados import (
    obter_configuracao_execucao_sot,
    obter_configuracao_origem,
)
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


def test_montar_contexto_templates_sot_inclui_data_e_process_id():
    contexto = JobGlueCargaSor.montar_contexto_templates_sot(
        {"PROCESS_ID": "contratos_sot", "DATA_REF": "20260415"}
    )

    assert contexto["PROCESS_ID"] == "contratos_sot"
    assert contexto["DATA_REF"] == "20260415"
    assert contexto["ANO_MES_DATA_REF"] == "202604"
    assert contexto["ANO_MES_ANTERIOR"] == "202603"


def test_obter_configuracao_execucao_sot_seleciona_por_process_id():
    contexto = JobGlueCargaSor.montar_contexto_templates_sot(
        {"PROCESS_ID": "contratos_sot", "DATA_REF": "20260415"}
    )

    execucao = obter_configuracao_execucao_sot("contratos_sot", contexto)

    assert execucao["modo_execucao"] == "sot"
    assert execucao["process_id"] == "contratos_sot"
    assert execucao["sql_path"] == "sql/sot/contratos.sql"
    assert execucao["post_columns"] == {"process_id": "contratos_sot"}
    assert execucao["fontes"][0]["filtro_origem"] == "data_base = '20260415'"


def test_obter_configuracao_destino_sot_usa_process_id():
    destino = obter_configuracao_destino_sot(
        "contratos_sot",
        {"AMBIENTE": "dev"},
    )

    assert destino["modo_execucao"] == "sot"
    assert destino["nome_database_destino"] == "data_base_dev"
    assert destino["nome_tabela_destino"] == "import"
    assert destino["particao_tabela_destino"] == ["data_base"]


def test_resolver_argumentos_job_sot_exige_process_id(monkeypatch):
    def fake_get_resolved_options(_argv, nomes):
        if nomes == ["JOB_NAME", "MODO_EXECUCAO", "AMBIENTE", "DBLOCAL"]:
            return {
                "JOB_NAME": "job",
                "MODO_EXECUCAO": "sot",
                "AMBIENTE": "dev",
                "DBLOCAL": "false",
            }
        if nomes == ["PROCESS_ID"]:
            raise GlueArgumentError("PROCESS_ID ausente")
        raise GlueArgumentError("argumento opcional ausente")

    monkeypatch.setattr(main, "getResolvedOptions", fake_get_resolved_options)
    job = object.__new__(JobGlueCargaSor)

    with pytest.raises(GlueArgumentError, match="PROCESS_ID"):
        job.resolver_argumentos_job()


def test_resolver_argumentos_job_sor_exige_ingestion_id(monkeypatch):
    def fake_get_resolved_options(_argv, nomes):
        if nomes == ["JOB_NAME", "MODO_EXECUCAO", "AMBIENTE", "DBLOCAL"]:
            return {
                "JOB_NAME": "job",
                "MODO_EXECUCAO": "sor",
                "AMBIENTE": "dev",
                "DBLOCAL": "false",
            }
        if nomes == ["INGESTION_ID"]:
            raise GlueArgumentError("INGESTION_ID ausente")
        raise GlueArgumentError("argumento opcional ausente")

    monkeypatch.setattr(main, "getResolvedOptions", fake_get_resolved_options)
    job = object.__new__(JobGlueCargaSor)

    with pytest.raises(GlueArgumentError, match="INGESTION_ID"):
        job.resolver_argumentos_job()


def test_resolver_argumentos_job_rejeita_modo_invalido(monkeypatch):
    def fake_get_resolved_options(_argv, _nomes):
        return {
            "JOB_NAME": "job",
            "MODO_EXECUCAO": "generico",
            "AMBIENTE": "dev",
            "DBLOCAL": "false",
        }

    monkeypatch.setattr(main, "getResolvedOptions", fake_get_resolved_options)
    job = object.__new__(JobGlueCargaSor)

    with pytest.raises(ValueError, match="sot"):
        job.resolver_argumentos_job()


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
