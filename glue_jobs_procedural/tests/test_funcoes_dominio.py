from __future__ import annotations

import pytest

from glue_jobs_lib import common, sor, sot, spec


class FakeDynamoResource:
    def __init__(self, items: dict[tuple[str, str], dict]):
        self.items = items

    def Table(self, _table_name):
        return FakeDynamoTable(self.items)


class FakeDynamoTable:
    def __init__(self, items: dict[tuple[str, str], dict]):
        self.items = items

    def get_item(self, Key):
        item = self.items.get((Key["PK"], Key["SK"]))
        return {"Item": item} if item else {}

    def query(self, **kwargs):
        pk = kwargs["ExpressionAttributeValues"][":pk"]
        prefixo_sk = kwargs["ExpressionAttributeValues"][":prefixo_sk"]
        return {
            "Items": [
                item
                for (item_pk, item_sk), item in self.items.items()
                if item_pk == pk and item_sk.startswith(prefixo_sk)
            ]
        }


def test_sot_exige_apenas_process_id_como_argumento_de_negocio(monkeypatch):
    valores = {
        "JOB_NAME": "job",
        "PROCESS_ID": "contratos_sot",
        "AMBIENTE": "dev",
        "DBLOCAL": "false",
    }

    def fake_resolver(nomes):
        faltantes = [nome for nome in nomes if nome not in valores]
        if faltantes:
            raise ValueError("Faltantes: " + ", ".join(faltantes))
        return {nome: valores[nome] for nome in nomes}

    monkeypatch.setattr(common, "resolver_argumentos_obrigatorios", fake_resolver)

    assert sot.resolver_argumentos_job() == valores


def test_sot_monta_predicate_por_input_lock_e_metadata_processo():
    args = {
        "PROCESS_ID": "proc_seguros_202604_001",
    }
    config_origem = {
        "execucoes": [
            {
                "dominio": "sot",
                "process_name": "seguros",
                "sql_path": "sql/contratos.sql",
                "fontes": [
                    {
                        "tipo_origem": "catalogo",
                        "database_origem": "sor_db",
                        "tabela_origem": "contratos",
                        "filtro_origem": (
                            "ingestion_id = '{INGESTION_ID}' AND "
                            "process_id = '{PROCESS_ID}' AND data_ref = '{DATA_REF}'"
                        ),
                    }
                ],
            }
        ]
    }
    dynamodb = FakeDynamoResource(
        {
            (
                "PROCESS#proc_seguros_202604_001",
                "METADATA",
            ): {
                "process_id": "proc_seguros_202604_001",
                "process_name": "seguros",
                "data_referencia": "202604",
            },
            (
                "PROCESS#proc_seguros_202604_001",
                "INPUT_LOCK#contratos",
            ): {
                "sor_table_name": "contratos",
                "ingestion_id": "ing_001",
            },
        }
    )

    contexto = sot.montar_contexto_templates(args, dynamodb_resource=dynamodb)
    execucao = sot.obter_execucao(config_origem, args, contexto, dynamodb_resource=dynamodb)
    fontes = sot.montar_fontes(execucao)

    assert fontes[0]["predicate_final"] == (
        "ingestion_id = 'ing_001' AND process_id = 'proc_seguros_202604_001' AND data_ref = '202604'"
    )
    assert contexto["DATA_REF"] == "202604"
    assert sot.obter_colunas_tecnicas(execucao, args) == {
        "process_id": "proc_seguros_202604_001"
    }


def test_sot_fonte_sem_ingestion_id_nao_exige_input_lock():
    args = {"PROCESS_ID": "proc_seguros_202604_001"}
    config_origem = {
        "execucoes": [
            {
                "dominio": "sot",
                "process_name": "seguros",
                "sql_path": "sql/contratos.sql",
                "fontes": [
                    {
                        "tipo_origem": "catalogo",
                        "database_origem": "sot_db",
                        "tabela_origem": "processos_auxiliares",
                        "filtro_origem": "process_id = '{PROCESS_ID}' AND data_ref = '{DATA_REF}'",
                    }
                ],
            }
        ]
    }
    dynamodb = FakeDynamoResource(
        {
            (
                "PROCESS#proc_seguros_202604_001",
                "METADATA",
            ): {
                "process_id": "proc_seguros_202604_001",
                "process_name": "seguros",
                "data_referencia": "202604",
            },
        }
    )

    contexto = sot.montar_contexto_templates(args, dynamodb_resource=dynamodb)
    execucao = sot.obter_execucao(config_origem, args, contexto, dynamodb_resource=dynamodb)
    fontes = sot.montar_fontes(execucao)

    assert fontes[0]["predicate_final"] == (
        "process_id = 'proc_seguros_202604_001' AND data_ref = '202604'"
    )


def test_sot_falha_quando_fonte_exige_ingestion_id_sem_input_lock():
    args = {"PROCESS_ID": "proc_seguros_202604_001"}
    config_origem = {
        "execucoes": [
            {
                "dominio": "sot",
                "process_name": "seguros",
                "sql_path": "sql/contratos.sql",
                "fontes": [
                    {
                        "tipo_origem": "catalogo",
                        "database_origem": "sor_db",
                        "tabela_origem": "contratos",
                        "filtro_origem": "ingestion_id = '{INGESTION_ID}'",
                    }
                ],
            }
        ]
    }
    dynamodb = FakeDynamoResource(
        {
            (
                "PROCESS#proc_seguros_202604_001",
                "METADATA",
            ): {
                "process_id": "proc_seguros_202604_001",
                "process_name": "seguros",
                "data_referencia": "202604",
            },
        }
    )

    contexto = sot.montar_contexto_templates(args, dynamodb_resource=dynamodb)

    with pytest.raises(ValueError, match="INPUT_LOCK"):
        sot.obter_execucao(config_origem, args, contexto, dynamodb_resource=dynamodb)


def test_spec_busca_ingestion_id_por_input_lock():
    args = {
        "PROCESS_ID": "proc_seguros_202604_001",
        "AMBIENTE": "dev",
        "DBLOCAL": "false",
    }
    config_origem = {
        "execucoes": [
            {
                "dominio": "spec",
                "process_name": "seguros",
                "sql_path": "sql/contratos.sql",
                "fontes": [
                    {
                        "tipo_origem": "catalogo",
                        "database_origem": "sot_db",
                        "tabela_origem": "contratos_sot",
                        "filtro_origem": "ingestion_id = '{INGESTION_ID}'",
                    }
                ],
            }
        ]
    }
    dynamodb = FakeDynamoResource(
        {
            (
                "PROCESS#proc_seguros_202604_001",
                "METADATA",
            ): {
                "process_id": "proc_seguros_202604_001",
                "process_name": "seguros",
                "data_referencia": "202604",
            },
            (
                "PROCESS#proc_seguros_202604_001",
                "INPUT_LOCK#contratos_sot",
            ): {
                "sor_table_name": "contratos_sot",
                "ingestion_id": "ing_spec_001",
            },
        }
    )

    contexto = spec.montar_contexto_templates(args, dynamodb_resource=dynamodb)
    execucao = spec.obter_execucao(
        config_origem,
        args,
        contexto,
        dynamodb_resource=dynamodb,
    )
    fontes = spec.montar_fontes(execucao)

    assert fontes[0]["predicate_final"] == "ingestion_id = 'ing_spec_001'"


def test_spec_nao_busca_ingestion_id_quando_config_nao_usa_placeholder():
    args = {
        "PROCESS_ID": "contratos_spec",
        "AMBIENTE": "dev",
        "DBLOCAL": "false",
    }
    config_origem = {
        "execucoes": [
            {
                "dominio": "spec",
                "process_name": "seguros",
                "sql_path": "sql/contratos.sql",
                "fontes": [
                    {
                        "tipo_origem": "catalogo",
                        "database_origem": "sot_db",
                        "tabela_origem": "contratos_sot",
                        "filtro_origem": "process_id = '{PROCESS_ID}'",
                    }
                ],
            }
        ]
    }
    dynamodb = FakeDynamoResource(
        {
            (
                "PROCESS#contratos_spec",
                "METADATA",
            ): {
                "process_id": "contratos_spec",
                "process_name": "seguros",
                "data_referencia": "202604",
            },
        }
    )

    contexto = spec.montar_contexto_templates(args, dynamodb_resource=dynamodb)
    execucao = spec.obter_execucao(config_origem, args, contexto, dynamodb_resource=dynamodb)
    fontes = spec.montar_fontes(execucao)

    assert fontes[0]["predicate_final"] == "process_id = 'contratos_spec'"


def test_spec_falha_com_multiplos_destinos():
    args = {"PROCESS_ID": "contratos_spec", "AMBIENTE": "dev"}
    config_destino = {
        "catalogo": {
            "group_files": "inPartition",
            "group_size_bytes": 1,
            "use_glue_parquet_writer": True,
            "compression": "snappy",
            "block_size_bytes": 1,
        },
        "ambientes": {"dev": {"s3_destino": "s3://bucket/spec"}},
        "destinos": [
            {
                "dominio": "spec",
                "process_name": "seguros",
                "nome_database_destino": "spec_db",
                "nome_tabela_destino": "a",
            },
            {
                "dominio": "spec",
                "process_name": "seguros",
                "nome_database_destino": "spec_db",
                "nome_tabela_destino": "b",
            },
        ],
    }
    contexto = {"PROCESS_NAME": "seguros"}

    with pytest.raises(ValueError, match="exatamente um destino"):
        spec.obter_destino(config_destino, args, contexto)


def test_sor_busca_metadata_e_monta_contexto_partitions():
    dynamodb = FakeDynamoResource(
        {
            (
                "INGESTION_ID#ing_100",
                "METADATA",
            ): {
                "ingestion_id": "ing_100",
                "process_name": "seguros",
                "source_database_name": "origem_db",
                "source_table_name": "contrato",
                "sor_database_name": "sor_db",
                "sor_table_name": "seguros_contratos",
                "partitions": {"data-referencia": "202604"},
                "status": "PROCESSING_SOR",
            }
        }
    )

    metadata = sor.obter_metadata_ingestao(dynamodb, "ing_100")
    contexto = sor.montar_contexto_templates(metadata)

    assert metadata["status"] == "PROCESSING_SOR"
    assert contexto["SOURCE_TABLE_NAME"] == "contrato"
    assert contexto["PARTITION_DATA_REFERENCIA"] == "202604"


def test_common_argumento_predicate_por_tabela_origem_vence_config():
    fonte = {
        "tipo_origem": "catalogo",
        "tabela_origem": "contratos",
        "filtro_origem": "process_id = 'config'",
    }

    predicate = common.resolver_predicate_fonte(
        fonte,
        predicates_argumento={"contratos": "process_id = 'arg'"},
    )

    assert predicate == "process_id = 'arg'"
