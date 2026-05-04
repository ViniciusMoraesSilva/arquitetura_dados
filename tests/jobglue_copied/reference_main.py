"""Testes unitários do módulo principal do job Glue com pytest."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest import mock

from test._dependency_stubs import install_dependency_stubs

install_dependency_stubs()
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import main  # noqa: E402


def test_resolver_argumento_predicates_retorna_dicionario() -> None:
    job = main.JobGlue.__new__(main.JobGlue)

    with mock.patch.object(
        main,
        "getResolvedOptions",
        return_value={
            "TABLE_PREDICATES": (
                "tabela_contrato::data_base = '202501';"
                "tabela_cliente::sigla = 'A'"
            )
        },
    ):
        predicates = job.resolver_argumento_predicates()

    assert predicates == {
        "tabela_contrato": "data_base = '202501'",
        "tabela_cliente": "sigla = 'A'",
    }


def test_resolver_predicate_tabela_prioriza_table_predicates_sobre_ddb() -> None:
    job = main.JobGlue.__new__(main.JobGlue)
    job.predicates = {"tabela_contrato": "data_base = '202501'"}
    job.predicates_override_ddb = {"tabela_contrato": "sigla = 'A'"}

    predicate = job.resolver_predicate_tabela(
        "tabela_contrato",
        "data_base = '202412'",
    )

    assert predicate == "data_base = '202501'"


def test_resolver_predicate_tabela_usa_override_ddb_quando_nao_houver_table_predicates() -> None:
    job = main.JobGlue.__new__(main.JobGlue)
    job.predicates = None
    job.predicates_override_ddb = {"tabela_contrato": "sigla = 'A'"}

    predicate = job.resolver_predicate_tabela(
        "tabela_contrato",
        "data_base = '202412'",
    )

    assert predicate == "sigla = 'A'"


def test_resolver_predicate_tabela_faz_fallback_para_predicate_padrao() -> None:
    job = main.JobGlue.__new__(main.JobGlue)
    job.predicates = None
    job.predicates_override_ddb = {}

    predicate = job.resolver_predicate_tabela(
        "tabela_contrato",
        "data_base = '202412'",
    )

    assert predicate == "data_base = '202412'"


@mock.patch.object(main.JobGlue, "inicializar_o_contexto")
@mock.patch.object(main.JobGlue, "carregar_predicates_override_ddb")
@mock.patch.object(main.JobGlue, "resolver_flag_predicate_override_ddb")
@mock.patch.object(main.JobGlue, "resolver_argumento_predicates")
@mock.patch.object(main.JobGlue, "resolver_argumentos_job")
def test_init_faz_preload_predicates_override_ddb_uma_vez(
    mock_resolver_argumentos_job: mock.Mock,
    mock_resolver_argumento_predicates: mock.Mock,
    mock_resolver_flag_predicate_override_ddb: mock.Mock,
    mock_carregar_predicates_override_ddb: mock.Mock,
    mock_inicializar_o_contexto: mock.Mock,
) -> None:
    mock_resolver_argumentos_job.return_value = {
        "JOB_NAME": "job_teste",
        "DATA_REF": "202501",
        "TIPO_PROCESSAMENTO": "producao",
        "GLUE_SQL_LOCATION": "s3://bucket/scripts",
        "AMBIENTE": "dev",
        "DBLOCAL": "false",
    }
    mock_resolver_argumento_predicates.return_value = None
    mock_resolver_flag_predicate_override_ddb.return_value = True
    mock_carregar_predicates_override_ddb.return_value = {
        "tabela_contrato": "data_base = '202501'"
    }
    mock_inicializar_o_contexto.return_value = (
        mock.Mock(),
        mock.Mock(),
        mock.Mock(),
        mock.Mock(),
        mock.Mock(),
    )

    job = main.JobGlue()

    mock_carregar_predicates_override_ddb.assert_called_once_with()
    assert job.predicates_override_ddb == {
        "tabela_contrato": "data_base = '202501'"
    }


@mock.patch.object(main, "obter_predicate_override_ddb")
@mock.patch.object(main.boto3, "resource")
def test_carregar_predicates_override_ddb_busca_item_unico_por_execucao(
    mock_boto3_resource: mock.Mock,
    mock_obter_predicate_override_ddb: mock.Mock,
) -> None:
    job = main.JobGlue.__new__(main.JobGlue)
    job.usar_predicate_override_ddb = True
    job.args = {"JOB_NAME": "job_teste", "DATA_REF": "202501"}
    mock_boto3_resource.return_value = mock.Mock()
    mock_obter_predicate_override_ddb.return_value = {
        "tabela_contrato": "data_base = '202501'"
    }

    predicates = job.carregar_predicates_override_ddb()

    assert predicates == {"tabela_contrato": "data_base = '202501'"}
    mock_obter_predicate_override_ddb.assert_called_once_with(
        dynamodb_resource=mock_boto3_resource.return_value,
        job_name="job_teste",
        data_ref="202501",
    )


def test_montar_fontes_retorna_lista_declarativa() -> None:
    job = main.JobGlue.__new__(main.JobGlue)
    job.args = {"DATA_REF": "202501"}
    job.carregar_config_origem_dados = mock.Mock(
        return_value={
            "catalogo": {
                "fontes": [
                    {
                        "tabela_origem": "tabela_contrato",
                        "descricao_origem": "Tabela de Contrato",
                        "database_origem": "data_base_dev",
                        "filtro_origem": "data_base = '{DATA_REF}'",
                    }
                ]
            },
            "csv": {
                "fontes": []
            },
        }
    )

    fontes = job.montar_fontes()

    assert len(fontes) == 1
    assert fontes[0]["table"] == "tabela_contrato"
    assert fontes[0]["partition"] == "data_base = '202501'"
    assert fontes[0]["tipo_origem"] == "catalogo"
    assert fontes[0]["database"] == "data_base_dev"


def test_carregar_config_origem_dados_retorna_config_do_json(tmp_path: Path) -> None:
    job = main.JobGlue.__new__(main.JobGlue)
    arquivo_config = tmp_path / "config_origem_dados.json"
    arquivo_config.write_text(
        json.dumps(
            {
                "catalogo": {
                    "fontes": [
                        {
                            "tabela_origem": "tabela_contrato",
                        }
                    ]
                },
                "csv": {"fontes": []},
            }
        ),
        encoding="utf-8",
    )

    with mock.patch.object(main, "CONFIG_ORIGEM_DADOS_PATH", arquivo_config):
        config = job.carregar_config_origem_dados()

    assert config == {
        "catalogo": {"fontes": [{"tabela_origem": "tabela_contrato"}]},
        "csv": {"fontes": []},
    }


def test_listar_origens_configuradas_consolida_blocos_catalogo_e_csv() -> None:
    job = main.JobGlue.__new__(main.JobGlue)
    job.carregar_config_origem_dados = mock.Mock(
        return_value={
            "catalogo": {
                "fontes": [{"tabela_origem": "tabela_1"}]
            },
            "csv": {
                "fontes": [
                    {
                        "nome_view": "view_csv_1",
                        "header": False,
                        "delimiter": ";",
                    }
                ]
            },
        }
    )

    origens = job.listar_origens_configuradas()

    assert origens == [
        {"bloco_origem": "catalogo", "tabela_origem": "tabela_1"},
        {
            "bloco_origem": "csv",
            "header": False,
            "delimiter": ";",
            "nome_view": "view_csv_1",
        },
    ]


def test_montar_contexto_templates_origem_resolve_mes_anterior() -> None:
    job = main.JobGlue.__new__(main.JobGlue)
    job.args = {"DATA_REF": "202501"}

    contexto = job.montar_contexto_templates_origem()

    assert contexto["DATA_REF"] == "202501"
    assert contexto["ANO_MES_DATA_REF"] == "202501"
    assert contexto["MES_ANTERIOR"] == "12"
    assert contexto["ANO_MES_ANTERIOR"] == "202412"


def test_resolver_origem_configurada_resolve_placeholders_em_catalogo_e_csv() -> None:
    job = main.JobGlue.__new__(main.JobGlue)
    contexto = {
        "DATA_REF": "202501",
        "ANO_MES_ANTERIOR": "202412",
    }

    origem_catalogo = job.resolver_origem_configurada(
        {
            "bloco_origem": "catalogo",
            "tabela_origem": "tabela_contrato",
            "filtro_origem": "data_base = '{DATA_REF}'",
        },
        contexto,
    )
    origem_csv = job.resolver_origem_configurada(
        {
            "bloco_origem": "csv",
            "nome_view": "tabela_csv",
            "caminho_s3_origem": "s3://bucket/base_{ANO_MES_ANTERIOR}.csv",
        },
        contexto,
    )

    assert origem_catalogo["filtro_origem"] == "data_base = '202501'"
    assert origem_csv["caminho_s3_origem"] == "s3://bucket/base_202412.csv"


def test_resolver_origem_configurada_erro_quando_placeholder_desconhecido() -> None:
    job = main.JobGlue.__new__(main.JobGlue)

    try:
        job.resolver_origem_configurada(
            {
                "bloco_origem": "catalogo",
                "tabela_origem": "tabela_contrato",
                "filtro_origem": "data_base = '{DATA_INEXISTENTE}'",
            },
            {"DATA_REF": "202501"},
        )
    except ValueError as exc:
        assert (
            str(exc)
            == "Placeholder desconhecido na configuracao da origem para o campo filtro_origem: DATA_INEXISTENTE"
        )
    else:  # pragma: no cover - garantia explícita
        raise AssertionError("Era esperado ValueError para placeholder desconhecido.")


def test_mapear_origem_para_carregar_catalogo_no_bloco_catalogo() -> None:
    job = main.JobGlue.__new__(main.JobGlue)

    origem = job.mapear_origem_para_carregar_catalogo(
        {
            "bloco_origem": "catalogo",
            "tabela_origem": "tabela_contrato",
            "descricao_origem": "Tabela de Contrato",
            "database_origem": "data_base_dev",
            "filtro_origem": "data_base = '202501'",
        }
    )

    assert origem == {
        "table": "tabela_contrato",
        "description": "Tabela de Contrato",
        "database": "data_base_dev",
        "partition": "data_base = '202501'",
        "tipo_origem": "catalogo",
    }


def test_mapear_origem_para_carregar_catalogo_no_bloco_csv() -> None:
    job = main.JobGlue.__new__(main.JobGlue)

    origem = job.mapear_origem_para_carregar_catalogo(
        {
            "bloco_origem": "csv",
            "nome_view": "tabela_csv",
            "descricao_origem": "Tabela CSV",
            "caminho_s3_origem": "s3://bucket/csv/tabela.csv",
            "filtro_origem": "data_base = '202501'",
            "header": False,
            "delimiter": ";",
            "infer_schema": False,
            "encoding": "latin1",
            "quote": "'",
            "escape": "/",
        }
    )

    assert origem == {
        "table": "tabela_csv",
        "description": "Tabela CSV",
        "caminho_s3": "s3://bucket/csv/tabela.csv",
        "partition": "data_base = '202501'",
        "tipo_origem": "csv",
        "header": False,
        "delimiter": ";",
        "infer_schema": False,
        "encoding": "latin1",
        "quote": "'",
        "escape": "/",
    }


def test_mapear_origem_para_carregar_catalogo_no_bloco_csv_com_fallback_padrao() -> None:
    job = main.JobGlue.__new__(main.JobGlue)

    origem = job.mapear_origem_para_carregar_catalogo(
        {
            "bloco_origem": "csv",
            "nome_view": "tabela_csv",
            "descricao_origem": "Tabela CSV",
            "caminho_s3_origem": "s3://bucket/csv/tabela.csv",
        }
    )

    assert origem == {
        "table": "tabela_csv",
        "description": "Tabela CSV",
        "caminho_s3": "s3://bucket/csv/tabela.csv",
        "partition": None,
        "tipo_origem": "csv",
        "header": True,
        "delimiter": ",",
        "infer_schema": True,
        "encoding": "UTF-8",
        "quote": '"',
        "escape": "\\",
    }


def test_carregar_fontes_itera_sobre_montar_fontes() -> None:
    job = main.JobGlue.__new__(main.JobGlue)
    job.montar_fontes = mock.Mock(
        return_value=[
            {"table": "tabela_1", "description": "Tabela 1"},
            {"table": "tabela_2", "description": "Tabela 2"},
        ]
    )
    job.carregar_catalogo = mock.Mock()

    job.carregar_fontes()

    assert job.carregar_catalogo.call_count == 2
    job.carregar_catalogo.assert_any_call(
        table="tabela_1",
        description="Tabela 1",
    )
    job.carregar_catalogo.assert_any_call(
        table="tabela_2",
        description="Tabela 2",
    )


def test_carregar_dados_glue_catalog_passa_group_files_e_group_size() -> None:
    job = main.JobGlue.__new__(main.JobGlue)
    job.resolver_predicate_tabela = mock.Mock(return_value="data_base = '202501'")
    job.glue_context = mock.Mock()
    dataframe = mock.Mock()
    job.glue_context.create_data_frame.from_catalog.return_value = dataframe

    with mock.patch.object(
        main,
        "obter_configuracao_leitura_origem",
        return_value={"group_files": "inPartition", "group_size_bytes": 134217728},
    ):
        resultado = job.carregar_dados_glue(
            tipo_origem="catalogo",
            table="tabela_contrato",
            description="Tabela de Contrato",
            database="data_base_dev",
            partition="data_base = '202412'",
        )

    assert resultado is dataframe
    job.glue_context.create_data_frame.from_catalog.assert_called_once_with(
        database="data_base_dev",
        table_name="tabela_contrato",
        useCatalogSchema=True,
        additional_options={
            "groupFiles": "inPartition",
            "groupSize": "134217728",
        },
        push_down_predicate="data_base = '202501'",
    )


def test_carregar_dados_glue_csv_usa_dynamic_frame_com_grouping() -> None:
    job = main.JobGlue.__new__(main.JobGlue)
    job.resolver_predicate_tabela = mock.Mock(return_value="data_base = '202501'")
    job.glue_context = mock.Mock()
    dynamic_frame = mock.Mock()
    dataframe = mock.Mock()
    dynamic_frame.toDF.return_value = dataframe
    job.glue_context.create_dynamic_frame.from_options.return_value = dynamic_frame

    with mock.patch.object(
        main,
        "obter_configuracao_leitura_origem",
        return_value={"group_files": "inPartition", "group_size_bytes": 134217728},
    ):
        resultado = job.carregar_dados_glue(
            tipo_origem="csv",
            table="tabela_contrato",
            description="Tabela CSV",
            caminho_s3="s3://bucket/csv/tabela_contrato.csv",
        )

    assert resultado is dataframe.filter.return_value
    job.glue_context.create_dynamic_frame.from_options.assert_called_once_with(
        connection_type="s3",
        connection_options={
            "paths": ["s3://bucket/csv/tabela_contrato.csv"],
            "groupFiles": "inPartition",
            "groupSize": "134217728",
        },
        format="csv",
        format_options={
            "withHeader": True,
            "separator": ",",
            "quoteChar": '"',
            "escaper": "\\",
            "inferSchema": True,
            "encoding": "UTF-8",
        },
    )
    dataframe.filter.assert_called_once_with("data_base = '202501'")


def test_carregar_dados_glue_erro_quando_tipo_origem_invalido() -> None:
    job = main.JobGlue.__new__(main.JobGlue)
    job.resolver_predicate_tabela = mock.Mock(return_value=None)

    with mock.patch.object(
        main,
        "obter_configuracao_leitura_origem",
        return_value={"group_files": "inPartition", "group_size_bytes": 134217728},
    ):
        try:
            job.carregar_dados_glue(
                tipo_origem="api",
                table="tabela_contrato",
                description="Tabela API",
            )
        except ValueError as exc:
            assert str(exc) == "tipo_origem invalido. Use 'catalogo' ou 'csv'."
        else:  # pragma: no cover - garantia explícita
            raise AssertionError("Era esperado ValueError para tipo_origem invalido.")
