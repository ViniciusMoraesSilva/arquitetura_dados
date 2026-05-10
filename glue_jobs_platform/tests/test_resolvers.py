from __future__ import annotations

import json
from pathlib import Path

import pytest

from glue_jobs_lib.arguments import GlueArguments
from glue_jobs_lib.resolvers import SorPlanResolver, SotPlanResolver, SpecPlanResolver


class FakeArguments(GlueArguments):
    def __init__(self, values: dict[str, str], optional: dict[str, str] | None = None):
        self.values = values
        self.optional = optional or {}

    def resolve_required(self, names: list[str]) -> dict[str, str]:
        missing = [name for name in names if name not in self.values]
        if missing:
            raise ValueError("Argumentos obrigatorios ausentes: " + ", ".join(missing))
        return {name: self.values[name] for name in names}

    def resolve_optional(self, name: str) -> str | None:
        return self.optional.get(name)


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


def write_json(path: Path, content: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(content), encoding="utf-8")
    return path


def source_config(tmp_path: Path, executions: list[dict]) -> Path:
    return write_json(
        tmp_path / "config_origem_dados.json",
        {
            "catalogo": {
                "group_files": "inPartition",
                "group_size_bytes": 134217728,
            },
            "execucoes": executions,
        },
    )


def destination_config(tmp_path: Path, destinations: list[dict]) -> Path:
    return write_json(
        tmp_path / "config_destino_dados.json",
        {
            "catalogo": {
                "group_files": "inPartition",
                "group_size_bytes": 134217728,
                "use_glue_parquet_writer": True,
                "compression": "snappy",
                "block_size_bytes": 134217728,
            },
            "destinos": destinations,
            "ambientes": {"dev": {"s3_destino": "s3://bucket-dev/base"}},
        },
    )


def test_sot_requires_process_data_ref_and_ingestion_id(tmp_path):
    resolver = SotPlanResolver(
        source_config_path=source_config(tmp_path, []),
        destination_config_path=destination_config(tmp_path, []),
        sql_base_path=tmp_path,
        arguments=FakeArguments(
            {
                "JOB_NAME": "job",
                "PROCESS_ID": "proc",
                "DATA_REF": "202604",
                "AMBIENTE": "dev",
                "DBLOCAL": "false",
            }
        ),
    )

    with pytest.raises(ValueError, match="INGESTION_ID"):
        resolver.resolve()


def test_sot_resolves_ingestion_and_process_placeholders(tmp_path):
    source_path = source_config(
        tmp_path,
        [
            {
                "domain": "sot",
                "process_id": "contratos_sot",
                "sql_path": "sql/contratos.sql",
                "fontes": [
                    {
                        "tipo_origem": "catalogo",
                        "database_origem": "sor_db",
                        "tabela_origem": "contratos",
                        "nome_view": "contratos",
                        "filtro_origem": (
                            "ingestion_id = '{INGESTION_ID}' AND "
                            "process_id = '{PROCESS_ID}' AND data_ref = '{DATA_REF}'"
                        ),
                    }
                ],
            }
        ],
    )
    destination_path = destination_config(
        tmp_path,
        [
            {
                "domain": "sot",
                "process_id": "contratos_sot",
                "tipo_saida_destino": "catalogo",
                "nome_database_destino": "sot_db",
                "nome_tabela_destino": "contratos_sot",
                "particao_tabela_destino": ["process_id"],
            }
        ],
    )

    plan = SotPlanResolver(
        source_config_path=source_path,
        destination_config_path=destination_path,
        sql_base_path=tmp_path,
        arguments=FakeArguments(
            {
                "JOB_NAME": "job",
                "PROCESS_ID": "contratos_sot",
                "DATA_REF": "202604",
                "INGESTION_ID": "ing_001",
                "AMBIENTE": "dev",
                "DBLOCAL": "false",
            }
        ),
    ).resolve()

    assert plan.domain == "sot"
    assert plan.sources[0].predicate == (
        "ingestion_id = 'ing_001' AND process_id = 'contratos_sot' AND data_ref = '202604'"
    )
    assert plan.post_columns == {"process_id": "contratos_sot"}


def test_spec_resolves_ingestion_id_from_dynamodb_when_config_uses_placeholder(tmp_path):
    source_path = source_config(
        tmp_path,
        [
            {
                "domain": "spec",
                "process_id": "contratos_spec",
                "sql_path": "sql/contratos.sql",
                "fontes": [
                    {
                        "tipo_origem": "catalogo",
                        "database_origem": "sot_db",
                        "tabela_origem": "contratos_sot",
                        "nome_view": "contratos_sot",
                        "filtro_origem": "ingestion_id = '{INGESTION_ID}'",
                    }
                ],
            }
        ],
    )
    destination_path = destination_config(
        tmp_path,
        [
            {
                "domain": "spec",
                "process_id": "contratos_spec",
                "tipo_saida_destino": "catalogo",
                "nome_database_destino": "spec_db",
                "nome_tabela_destino": "contratos_spec",
                "particao_tabela_destino": ["process_id"],
            }
        ],
    )
    dynamodb = FakeDynamoResource(
        {
            (
                "PROCESS_ID#contratos_spec",
                "DATA_REF#202604#SPEC_METADATA",
            ): {"ingestion_id": "ing_spec_001"}
        }
    )

    plan = SpecPlanResolver(
        source_config_path=source_path,
        destination_config_path=destination_path,
        sql_base_path=tmp_path,
        arguments=FakeArguments(
            {
                "JOB_NAME": "job",
                "PROCESS_ID": "contratos_spec",
                "DATA_REF": "202604",
                "AMBIENTE": "dev",
                "DBLOCAL": "false",
            }
        ),
        dynamodb_resource=dynamodb,
    ).resolve()

    assert plan.context["INGESTION_ID"] == "ing_spec_001"
    assert plan.sources[0].predicate == "ingestion_id = 'ing_spec_001'"


def test_spec_fails_with_multiple_destinations(tmp_path):
    source_path = source_config(
        tmp_path,
        [
            {
                "domain": "spec",
                "process_id": "contratos_spec",
                "sql_path": "sql/contratos.sql",
                "fontes": [],
            }
        ],
    )
    destination_path = destination_config(
        tmp_path,
        [
            {
                "domain": "spec",
                "process_id": "contratos_spec",
                "tipo_saida_destino": "catalogo",
                "nome_database_destino": "spec_db",
                "nome_tabela_destino": "a",
                "particao_tabela_destino": ["process_id"],
            },
            {
                "domain": "spec",
                "process_id": "contratos_spec",
                "tipo_saida_destino": "catalogo",
                "nome_database_destino": "spec_db",
                "nome_tabela_destino": "b",
                "particao_tabela_destino": ["process_id"],
            },
        ],
    )

    resolver = SpecPlanResolver(
        source_config_path=source_path,
        destination_config_path=destination_path,
        sql_base_path=tmp_path,
        arguments=FakeArguments(
            {
                "JOB_NAME": "job",
                "PROCESS_ID": "contratos_spec",
                "DATA_REF": "202604",
                "AMBIENTE": "dev",
                "DBLOCAL": "false",
            }
        ),
    )

    with pytest.raises(ValueError, match="exatamente um destino"):
        resolver.resolve()


def test_sor_resolves_metadata_by_ingestion_id(tmp_path):
    source_path = source_config(
        tmp_path,
        [
            {
                "domain": "sor",
                "process_name": "seguros",
                "sor_table_name": "seguros_contratos",
                "sql_path": "sql/seguros.sql",
                "fontes": [
                    {
                        "tipo_origem": "catalogo",
                        "database_origem": "{SOURCE_DATABASE_NAME}",
                        "tabela_origem": "{SOURCE_TABLE_NAME}",
                        "nome_view": "{SOURCE_TABLE_NAME}",
                        "filtro_origem": "data_ref = '{PARTITION_DATA_REF}'",
                    }
                ],
            }
        ],
    )
    destination_path = destination_config(
        tmp_path,
        [
            {
                "domain": "sor",
                "process_name": "seguros",
                "sor_table_name": "seguros_contratos",
                "tipo_saida_destino": "catalogo",
                "nome_database_destino": "sor_db",
                "nome_tabela_destino": "seguros_contratos",
                "particao_tabela_destino": ["ingestion_id"],
            }
        ],
    )
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
                "partitions": {"data_ref": "202604"},
                "status": "PROCESSING_SOR",
            }
        }
    )

    plan = SorPlanResolver(
        source_config_path=source_path,
        destination_config_path=destination_path,
        sql_base_path=tmp_path,
        arguments=FakeArguments(
            {
                "JOB_NAME": "job",
                "INGESTION_ID": "ing_100",
                "AMBIENTE": "dev",
                "DBLOCAL": "false",
            }
        ),
        dynamodb_resource=dynamodb,
    ).resolve()

    assert plan.domain == "sor"
    assert plan.sources[0].options["database_origem"] == "origem_db"
    assert plan.sources[0].predicate == "data_ref = '202604'"
    assert plan.post_columns == {"ingestion_id": "ing_100"}
