from pathlib import Path
import json
from unittest import mock

from tests.dependency_stubs import install_dependency_stubs

install_dependency_stubs()

from src.glue.jobglue_process_module import main as jobglue_main  # noqa: E402
from src.glue.jobglue_process_module.utils.executar_consultas_sql import executar_consultas_sql  # noqa: E402
from src.shared.control_plane import InMemoryControlPlane  # noqa: E402


def test_jobglue_sor_mode_uses_ingestion_id_and_rejects_process_id():
    job = jobglue_main.JobGlue.__new__(jobglue_main.JobGlue)

    with mock.patch.object(
        jobglue_main,
        "getResolvedOptions",
        side_effect=[
            {"JOB_MODE": "sor"},
            {
                "JOB_NAME": "carga-sor-generica",
                "INGESTION_ID": "ing_abc123",
                "GLUE_SQL_LOCATION": "s3://bucket/scripts/jobglue",
                "AMBIENTE": "dev",
                "CONTROL_TABLE_NAME": "pipeline_control_dev",
            },
        ],
    ):
        args = job.resolver_argumentos_job()

    assert args["JOB_MODE"] == "sor"
    assert args["INGESTION_ID"] == "ing_abc123"
    assert "PROCESS_ID" not in args


def test_jobglue_process_mode_uses_process_id_and_technical_config_only():
    job = jobglue_main.JobGlue.__new__(jobglue_main.JobGlue)

    with mock.patch.object(
        jobglue_main,
        "getResolvedOptions",
        side_effect=[
            {"JOB_MODE": "process"},
            {
                "JOB_NAME": "seguros-x",
                "PROCESS_ID": "proc_seguros_202604_deadbeef0000",
                "MODULE_CONFIG": "s3://bucket/scripts/modulos_seguros/x/config.json",
                "GLUE_SQL_LOCATION": "s3://bucket/scripts/jobglue",
                "AMBIENTE": "dev",
                "CONTROL_TABLE_NAME": "pipeline_control_dev",
            },
        ],
    ):
        args = job.resolver_argumentos_job()

    assert args["JOB_MODE"] == "process"
    assert args["PROCESS_ID"] == "proc_seguros_202604_deadbeef0000"
    assert "DATA_REF" not in args
    assert "TABLE_PREDICATES" not in args


def test_process_mode_resolves_sources_from_input_locks_without_data_ref():
    control = InMemoryControlPlane()
    process_id = "proc_seguros_202604_deadbeef0000"
    control.create_input_lock(process_id, "contratos", {"ingestion_id": "ing_contracts"})

    job = jobglue_main.JobGlue.__new__(jobglue_main.JobGlue)
    job.args = {"JOB_MODE": "process", "PROCESS_ID": process_id}
    job.control_plane = control

    source = job.resolver_fonte_processada(
        {
            "bloco_origem": "catalogo",
            "database_origem": "sor_dev",
            "tabela_origem": "contratos",
            "descricao_origem": "Contratos",
        }
    )

    assert source["partition"] == "ingestion_id='ing_contracts'"
    assert "data_referencia" not in repr(source).lower()


def test_copied_jobglue_assets_exist_without_terraform_copy():
    module_root = Path("src/glue/jobglue_process_module")

    assert (module_root / "main.py").exists()
    assert (module_root / "utils" / "executar_consultas_sql.py").exists()
    assert (module_root / "config" / "config_origem_dados.json").exists()
    assert (module_root / "sql" / "consultas.sql").exists()
    assert not (module_root / "terraform").exists()


def test_jobglue_main_removes_expensive_count_actions_from_mvp1_flow():
    source = Path("src/glue/jobglue_process_module/main.py").read_text()

    assert ".count()" not in source


def test_terraform_uses_copied_jobglue_main_for_all_glue_jobs():
    source = Path("infra/main.tf").read_text()

    assert "scripts/jobglue/main.py" in source
    assert "--JOB_MODE" in source
    assert "../src/glue/carga_sor_generica/job.py" not in source


def test_process_mode_loads_module_config_and_sql_from_jobglue_pattern(tmp_path):
    module_sql = tmp_path / "consultas.sql"
    module_sql.write_text("select 1;", encoding="utf-8")
    module_config = tmp_path / "config.json"
    module_config.write_text(
        json.dumps(
            {
                "module_name": "MODULO_X",
                "config_origem_dados": {
                    "catalogo": {
                        "group_files": "inPartition",
                        "group_size_bytes": 134217728,
                        "fontes": [
                            {
                                "database_origem": "sor_dev",
                                "tabela_origem": "contratos",
                                "descricao_origem": "Contratos",
                            }
                        ],
                    },
                    "csv": {"fontes": []},
                },
                "sql_file": str(module_sql),
            }
        ),
        encoding="utf-8",
    )

    job = jobglue_main.JobGlue.__new__(jobglue_main.JobGlue)
    job.args = {
        "JOB_MODE": "process",
        "PROCESS_ID": "proc_seguros_202604_deadbeef0000",
        "MODULE_CONFIG": str(module_config),
        "GLUE_SQL_LOCATION": "s3://bucket/scripts/jobglue",
    }
    spark = mock.Mock()
    dataframe = mock.Mock()
    dataframe.withColumn.return_value = dataframe
    job.spark = spark

    with mock.patch.object(jobglue_main, "executar_consultas_sql", return_value=dataframe) as executar:
        result = job.executar_consultas()

    assert job.carregar_config_origem_dados()["catalogo"]["fontes"][0]["tabela_origem"] == "contratos"
    executar.assert_called_once_with(
        spark,
        str(module_sql),
        template_context={
            "PROCESS_ID": "proc_seguros_202604_deadbeef0000",
            "INGESTION_ID": "",
        },
    )
    assert result is dataframe


def test_sql_executor_applies_process_id_template(tmp_path):
    sql_file = tmp_path / "consultas.sql"
    sql_file.write_text("select '${PROCESS_ID}' as process_id;", encoding="utf-8")
    dataframe = mock.Mock()
    spark = mock.Mock()
    spark.sql.return_value = dataframe

    executar_consultas_sql(
        spark,
        str(sql_file),
        template_context={"PROCESS_ID": "proc_seguros_202604_deadbeef0000"},
    )

    spark.sql.assert_called_once_with(
        "select 'proc_seguros_202604_deadbeef0000' as process_id"
    )
