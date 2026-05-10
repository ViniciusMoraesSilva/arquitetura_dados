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


def test_sot_exige_ingestion_id(monkeypatch):
    valores = {
        "JOB_NAME": "job",
        "PROCESS_ID": "contratos_sot",
        "DATA_REF": "202604",
        "AMBIENTE": "dev",
        "DBLOCAL": "false",
    }

    def fake_resolver(nomes):
        faltantes = [nome for nome in nomes if nome not in valores]
        if faltantes:
            raise ValueError("Faltantes: " + ", ".join(faltantes))
        return {nome: valores[nome] for nome in nomes}

    monkeypatch.setattr(common, "resolver_argumentos_obrigatorios", fake_resolver)

    with pytest.raises(ValueError, match="INGESTION_ID"):
        sot.resolver_argumentos_job()


def test_sot_monta_predicate_por_ingestion_id_e_process_id():
    args = {
        "PROCESS_ID": "contratos_sot",
        "DATA_REF": "202604",
        "INGESTION_ID": "ing_001",
    }
    contexto = sot.montar_contexto_templates(args)
    config_origem = {
        "execucoes": [
            {
                "dominio": "sot",
                "process_id": "contratos_sot",
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

    execucao = sot.obter_execucao(config_origem, args, contexto)
    fontes = sot.montar_fontes(execucao)

    assert fontes[0]["predicate_final"] == (
        "ingestion_id = 'ing_001' AND process_id = 'contratos_sot' AND data_ref = '202604'"
    )
    assert sot.obter_colunas_tecnicas(execucao, args) == {
        "process_id": "contratos_sot"
    }


def test_spec_busca_ingestion_id_quando_config_usa_placeholder():
    args = {
        "PROCESS_ID": "contratos_spec",
        "DATA_REF": "202604",
        "AMBIENTE": "dev",
        "DBLOCAL": "false",
    }
    contexto = spec.montar_contexto_templates(args)
    config_origem = {
        "execucoes": [
            {
                "dominio": "spec",
                "process_id": "contratos_spec",
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
                "PROCESS_ID#contratos_spec",
                "DATA_REF#202604#SPEC_METADATA",
            ): {"ingestion_id": "ing_spec_001"}
        }
    )

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
        "DATA_REF": "202604",
        "AMBIENTE": "dev",
        "DBLOCAL": "false",
    }
    contexto = spec.montar_contexto_templates(args)
    config_origem = {
        "execucoes": [
            {
                "dominio": "spec",
                "process_id": "contratos_spec",
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

    execucao = spec.obter_execucao(config_origem, args, contexto)
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
                "process_id": "contratos_spec",
                "nome_database_destino": "spec_db",
                "nome_tabela_destino": "a",
            },
            {
                "dominio": "spec",
                "process_id": "contratos_spec",
                "nome_database_destino": "spec_db",
                "nome_tabela_destino": "b",
            },
        ],
    }

    with pytest.raises(ValueError, match="exatamente um destino"):
        spec.obter_destino(config_destino, args)


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
