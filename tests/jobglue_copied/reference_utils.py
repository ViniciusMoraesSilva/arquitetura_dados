"""Testes unitários dos utilitários do projeto com pytest."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest import mock

from test._dependency_stubs import install_dependency_stubs

install_dependency_stubs()
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from utils.config_destino_dados import (  # noqa: E402
    obter_configuracao_destino,
    obter_configuracao_destino_dados,
)
from utils.config_origem_dados import obter_configuracao_leitura_origem  # noqa: E402
from utils.executar_consultas_sql import (  # noqa: E402
    _separar_consultas_sql,
    executar_consultas_sql,
)
from utils.gravar_catalogo_glue import (  # noqa: E402
    _montar_caminho_arquivo_csv_destino,
    _montar_caminho_destino_s3,
    _obter_partition_keys_destino,
    gravar_catalogo_glue,
)
from utils.obter_predicate_override_ddb import obter_predicate_override_ddb  # noqa: E402


def test_separar_consultas_sql_remove_trechos_vazios() -> None:
    consultas = _separar_consultas_sql("select 1;; select 2;  ;")

    assert consultas == ["select 1", "select 2"]


def test_executar_consultas_sql_executa_e_registra_views(tmp_path: Path) -> None:
    arquivo_sql = tmp_path / "queries.sql"
    arquivo_sql.write_text("select 1;select 2;", encoding="utf-8")

    dataframe_1 = mock.Mock()
    dataframe_2 = mock.Mock()
    spark = mock.Mock()
    spark.sql.side_effect = [dataframe_1, dataframe_2]

    resultado = executar_consultas_sql(spark, str(arquivo_sql))

    assert resultado is dataframe_2
    dataframe_1.createOrReplaceTempView.assert_called_once_with("query_temp_1")
    dataframe_2.createOrReplaceTempView.assert_called_once_with("query_temp_2")


def test_retorna_predicates_quando_item_esta_habilitado() -> None:
    tabela = mock.Mock()
    tabela.get_item.return_value = {
        "Item": {
            "enabled": True,
            "predicates": {
                "tabela_contrato": "data_base = '202501'",
                "tabela_cliente": "sigla = 'A'",
            },
        }
    }
    dynamodb_resource = mock.Mock()
    dynamodb_resource.Table.return_value = tabela

    predicates = obter_predicate_override_ddb(
        dynamodb_resource=dynamodb_resource,
        job_name="job",
        data_ref="202501",
    )

    assert predicates == {
        "tabela_contrato": "data_base = '202501'",
        "tabela_cliente": "sigla = 'A'",
    }


def test_retorna_dicionario_vazio_quando_item_esta_desabilitado() -> None:
    tabela = mock.Mock()
    tabela.get_item.return_value = {"Item": {"enabled": False}}
    dynamodb_resource = mock.Mock()
    dynamodb_resource.Table.return_value = tabela

    predicates = obter_predicate_override_ddb(
        dynamodb_resource=dynamodb_resource,
        job_name="job",
        data_ref="202501",
    )

    assert predicates == {}


def test_retorna_dicionario_vazio_quando_item_nao_existe() -> None:
    tabela = mock.Mock()
    tabela.get_item.return_value = {}
    dynamodb_resource = mock.Mock()
    dynamodb_resource.Table.return_value = tabela

    predicates = obter_predicate_override_ddb(
        dynamodb_resource=dynamodb_resource,
        job_name="job",
        data_ref="202501",
    )

    assert predicates == {}


def test_obter_partition_keys_destino_valida_colunas_existentes() -> None:
    df_entrada = mock.Mock()
    df_entrada.columns = ["data_base", "valor"]

    partition_keys = _obter_partition_keys_destino(
        df_entrada,
        {"particao_tabela_destino": ["data_base"]},
    )

    assert partition_keys == ["data_base"]


def test_montar_caminho_destino_s3_concatena_tabela() -> None:
    caminho = _montar_caminho_destino_s3(
        {
            "s3_destino": "s3://bucket-dev",
            "nome_tabela_destino": "import_credito",
        }
    )

    assert caminho == "s3://bucket-dev/import_credito/"


def test_montar_caminho_arquivo_csv_destino_concatena_tabela_e_arquivo() -> None:
    caminho = _montar_caminho_arquivo_csv_destino(
        {
            "s3_destino": "s3://bucket-csv-dev",
            "nome_pasta_destino": "import_credito",
            "nome_arquivo_csv": "arquivo_final.csv",
        }
    )

    assert caminho == "s3://bucket-csv-dev/import_credito/arquivo_final.csv"


def test_obter_configuracao_destino_retorna_group_size_128mb() -> None:
    configuracao_destino = obter_configuracao_destino()

    assert configuracao_destino["group_files"] == "inPartition"
    assert configuracao_destino["group_size_bytes"] == 134217728
    assert configuracao_destino["block_size_bytes"] == 134217728


def test_obter_configuracao_leitura_origem_retorna_group_size_128mb() -> None:
    configuracao_leitura_origem = obter_configuracao_leitura_origem()

    assert configuracao_leitura_origem["group_files"] == "inPartition"
    assert configuracao_leitura_origem["group_size_bytes"] == 134217728




@mock.patch("utils.config_destino_dados.carregar_config_destino_dados")
def test_obter_configuracao_destino_dados_mescla_bloco_ativo_com_ambiente(
    mock_carregar_config_destino_dados: mock.Mock,
) -> None:
    mock_carregar_config_destino_dados.return_value = {
        "tipo_saida_destino": "csv",
        "catalogo": {
            "nome_database_destino": "data_base_dev",
            "nome_tabela_destino": "import_catalogo",
            "particao_tabela_destino": ["data_base"],
            "group_files": "inPartition",
            "group_size_bytes": 134217728,
            "use_glue_parquet_writer": True,
            "compression": "snappy",
            "block_size_bytes": 134217728,
        },
        "csv": {
            "nome_pasta_destino": "import_csv",
            "nome_arquivo_csv": "arquivo_final.csv",
        },
        "ambientes": {
            "dev": {"s3_destino": "s3://bucket-dev"},
        },
    }

    configuracao = obter_configuracao_destino_dados({"AMBIENTE": "dev"})

    assert configuracao == {
        "nome_pasta_destino": "import_csv",
        "nome_arquivo_csv": "arquivo_final.csv",
        "s3_destino": "s3://bucket-dev",
        "tipo_saida_destino": "csv",
    }


@mock.patch("utils.config_destino_dados.carregar_config_destino_dados")
def test_obter_configuracao_destino_dados_erro_quando_ambiente_nao_existe(
    mock_carregar_config_destino_dados: mock.Mock,
) -> None:
    mock_carregar_config_destino_dados.return_value = {
        "tipo_saida_destino": "csv",
        "catalogo": {},
        "csv": {},
        "ambientes": {"dev": {"s3_destino": "s3://bucket-dev"}},
    }

    try:
        obter_configuracao_destino_dados({"AMBIENTE": "prd"})
    except ValueError as exc:
        assert (
            str(exc)
            == "Configuracao de destino de dados nao encontrada para ambiente=prd."
        )
    else:  # pragma: no cover - garantia explícita
        raise AssertionError("Era esperado ValueError para ambiente inexistente.")


@mock.patch("utils.config_destino_dados.carregar_config_destino_dados")
def test_obter_configuracao_destino_dados_erro_quando_tipo_saida_invalido(
    mock_carregar_config_destino_dados: mock.Mock,
) -> None:
    mock_carregar_config_destino_dados.return_value = {
        "tipo_saida_destino": "json",
        "catalogo": {},
        "csv": {},
        "ambientes": {"dev": {"s3_destino": "s3://bucket-dev"}},
    }

    try:
        obter_configuracao_destino_dados({"AMBIENTE": "dev"})
    except ValueError as exc:
        assert (
            str(exc)
            == "Configuracao tipo_saida_destino invalida. Use 'catalogo' ou 'csv'."
        )
    else:  # pragma: no cover - garantia explícita
        raise AssertionError("Era esperado ValueError para tipo_saida_destino invalido.")


@mock.patch("utils.gravar_catalogo_glue.DynamicFrame")
@mock.patch("utils.gravar_catalogo_glue.obter_configuracao_destino")
@mock.patch("utils.gravar_catalogo_glue.obter_configuracao_destino_dados")
def test_gravar_catalogo_glue_escreve_no_sink_quando_tipo_saida_e_catalogo(
    mock_obter_configuracao_destino_dados: mock.Mock,
    mock_obter_configuracao_destino: mock.Mock,
    mock_dynamic_frame: mock.Mock,
) -> None:
    mock_obter_configuracao_destino_dados.return_value = {
        "tipo_saida_destino": "catalogo",
        "particao_tabela_destino": ["data_base"],
        "s3_destino": "s3://bucket-dev",
        "nome_tabela_destino": "import_credito",
        "nome_database_destino": "data_base_dev",
        "nome_arquivo_csv": "arquivo_final.csv",
    }
    mock_obter_configuracao_destino.return_value = {
        "group_files": "inPartition",
        "group_size_bytes": 134217728,
        "use_glue_parquet_writer": True,
        "compression": "snappy",
        "block_size_bytes": 134217728,
    }

    linha_particao = {"data_base": "202501"}
    select_result = mock.Mock()
    select_result.distinct.return_value.collect.return_value = [linha_particao]

    df_entrada = mock.Mock()
    df_entrada.columns = ["data_base", "valor"]
    df_entrada.select.return_value = select_result

    sink = mock.Mock()
    glue_context = mock.Mock()
    glue_context.getSink.return_value = sink

    gravar_catalogo_glue(
        glue_context=glue_context,
        df_entrada=df_entrada,
        args={"AMBIENTE": "dev"},
    )

    glue_context.purge_s3_path.assert_called_once_with(
        "s3://bucket-dev/import_credito/data_base=202501/",
        {"retentionPeriod": 0},
    )
    glue_context.getSink.assert_called_once_with(
        connection_type="s3",
        path="s3://bucket-dev/import_credito/",
        enableUpdateCatalog=True,
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=["data_base"],
        groupFiles="inPartition",
        groupSize="134217728",
    )
    sink.setFormat.assert_called_once_with(
        "parquet",
        useGlueParquetWriter=True,
        compression="snappy",
        blockSize=134217728,
    )
    sink.setCatalogInfo.assert_called_once_with(
        catalogDatabase="data_base_dev",
        catalogTableName="import_credito",
    )
    sink.writeFrame.assert_called_once()
    df_entrada.coalesce.assert_not_called()


@mock.patch("utils.gravar_catalogo_glue.boto3.client")
@mock.patch("utils.gravar_catalogo_glue.obter_configuracao_destino_dados")
def test_gravar_catalogo_glue_escreve_csv_com_header_true(
    mock_obter_configuracao_destino_dados: mock.Mock,
    mock_boto3_client: mock.Mock,
) -> None:
    mock_obter_configuracao_destino_dados.return_value = {
        "tipo_saida_destino": "csv",
        "particao_tabela_destino": ["data_base"],
        "s3_destino": "s3://bucket-csv-dev",
        "nome_pasta_destino": "import_credito",
        "nome_database_destino": "data_base_dev",
        "nome_arquivo_csv": "arquivo_final.csv",
        "incluir_cabecalho": True,
    }

    df_entrada = mock.Mock()
    df_entrada.columns = ["data_base", "valor"]
    df_coalescido = mock.Mock()
    writer = mock.Mock()
    mode_writer = mock.Mock()
    option_writer = mock.Mock()
    df_entrada.coalesce.return_value = df_coalescido
    df_coalescido.write = writer
    writer.mode.return_value = mode_writer
    mode_writer.option.return_value = option_writer

    glue_context = mock.Mock()
    s3_client = mock.Mock()
    s3_client.list_objects_v2.return_value = {
        "Contents": [
            {"Key": "import_credito/_tmp/part-0000.csv"},
            {"Key": "import_credito/_tmp/_SUCCESS"},
        ]
    }
    mock_boto3_client.return_value = s3_client

    gravar_catalogo_glue(
        glue_context=glue_context,
        df_entrada=df_entrada,
        args={"AMBIENTE": "dev"},
    )

    glue_context.getSink.assert_not_called()
    assert glue_context.purge_s3_path.call_args_list == [
        mock.call(
            "s3://bucket-csv-dev/import_credito/_tmp/",
            {"retentionPeriod": 0},
        ),
        mock.call(
            "s3://bucket-csv-dev/import_credito/_tmp/",
            {"retentionPeriod": 0},
        ),
    ]
    df_entrada.coalesce.assert_called_once_with(1)
    writer.mode.assert_called_once_with("overwrite")
    mode_writer.option.assert_called_once_with("header", True)
    option_writer.csv.assert_called_once_with(
        "s3://bucket-csv-dev/import_credito/_tmp/"
    )
    s3_client.list_objects_v2.assert_called_once_with(
        Bucket="bucket-csv-dev",
        Prefix="import_credito/_tmp/",
    )
    s3_client.delete_object.assert_called_once_with(
        Bucket="bucket-csv-dev",
        Key="import_credito/arquivo_final.csv",
    )
    s3_client.copy_object.assert_called_once_with(
        Bucket="bucket-csv-dev",
        Key="import_credito/arquivo_final.csv",
        CopySource={
            "Bucket": "bucket-csv-dev",
            "Key": "import_credito/_tmp/part-0000.csv",
        },
    )


@mock.patch("utils.gravar_catalogo_glue.boto3.client")
@mock.patch("utils.gravar_catalogo_glue.obter_configuracao_destino_dados")
def test_gravar_catalogo_glue_escreve_csv_com_header_false(
    mock_obter_configuracao_destino_dados: mock.Mock,
    mock_boto3_client: mock.Mock,
) -> None:
    mock_obter_configuracao_destino_dados.return_value = {
        "tipo_saida_destino": "csv",
        "particao_tabela_destino": ["data_base"],
        "s3_destino": "s3://bucket-csv-dev",
        "nome_pasta_destino": "import_credito",
        "nome_database_destino": "data_base_dev",
        "nome_arquivo_csv": "arquivo_final.csv",
        "incluir_cabecalho": False,
    }

    df_entrada = mock.Mock()
    df_entrada.columns = ["data_base", "valor"]
    df_coalescido = mock.Mock()
    writer = mock.Mock()
    mode_writer = mock.Mock()
    option_writer = mock.Mock()
    df_entrada.coalesce.return_value = df_coalescido
    df_coalescido.write = writer
    writer.mode.return_value = mode_writer
    mode_writer.option.return_value = option_writer

    glue_context = mock.Mock()
    s3_client = mock.Mock()
    s3_client.list_objects_v2.return_value = {
        "Contents": [
            {"Key": "import_credito/_tmp/part-0000.csv"},
        ]
    }
    mock_boto3_client.return_value = s3_client

    gravar_catalogo_glue(
        glue_context=glue_context,
        df_entrada=df_entrada,
        args={"AMBIENTE": "dev"},
    )

    glue_context.getSink.assert_not_called()
    mode_writer.option.assert_called_once_with("header", False)
    option_writer.csv.assert_called_once_with(
        "s3://bucket-csv-dev/import_credito/_tmp/"
    )


@mock.patch("utils.gravar_catalogo_glue.boto3.client")
@mock.patch("utils.gravar_catalogo_glue.obter_configuracao_destino_dados")
def test_gravar_catalogo_glue_escreve_csv_com_fallback_header_true(
    mock_obter_configuracao_destino_dados: mock.Mock,
    mock_boto3_client: mock.Mock,
) -> None:
    mock_obter_configuracao_destino_dados.return_value = {
        "tipo_saida_destino": "csv",
        "particao_tabela_destino": ["data_base"],
        "s3_destino": "s3://bucket-csv-dev",
        "nome_pasta_destino": "import_credito",
        "nome_database_destino": "data_base_dev",
        "nome_arquivo_csv": "arquivo_final.csv",
    }

    df_entrada = mock.Mock()
    df_entrada.columns = ["data_base", "valor"]
    df_coalescido = mock.Mock()
    writer = mock.Mock()
    mode_writer = mock.Mock()
    option_writer = mock.Mock()
    df_entrada.coalesce.return_value = df_coalescido
    df_coalescido.write = writer
    writer.mode.return_value = mode_writer
    mode_writer.option.return_value = option_writer

    glue_context = mock.Mock()
    s3_client = mock.Mock()
    s3_client.list_objects_v2.return_value = {
        "Contents": [
            {"Key": "import_credito/_tmp/part-0000.csv"},
        ]
    }
    mock_boto3_client.return_value = s3_client

    gravar_catalogo_glue(
        glue_context=glue_context,
        df_entrada=df_entrada,
        args={"AMBIENTE": "dev"},
    )

    glue_context.getSink.assert_not_called()
    mode_writer.option.assert_called_once_with("header", True)
    option_writer.csv.assert_called_once_with(
        "s3://bucket-csv-dev/import_credito/_tmp/"
    )
