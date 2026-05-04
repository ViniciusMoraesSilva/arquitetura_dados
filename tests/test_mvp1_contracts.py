import json
import re
from pathlib import Path

import pytest

from src.glue.carga_sor_generica.job import build_catalog_pushdown_predicate, resolve_sor_load_plan
from src.glue.jobglue_process_module.resolver import build_ingestion_predicate, resolve_locked_sources
from src.lambdas.atualizar_step_status import handler as update_step_status
from src.lambdas.finalizar_carga_sor_falha import handler as fail_ingestion
from src.lambdas.finalizar_carga_sor_sucesso import handler as succeed_ingestion
from src.lambdas.preparar_full_generica import handler as prepare_full
from src.lambdas.preparar_ingestao import handler as prepare_ingestion
from src.shared.control_plane import DuplicateItemError, InMemoryControlPlane
from src.shared.ids import new_ingestion_id, new_process_id
from src.shared.time_utils import utc_now_iso, validate_data_referencia


def test_ids_and_dates_follow_mvp1_contracts():
    assert re.fullmatch(r"ing_[0-9a-f]{12}", new_ingestion_id(lambda: "abcdef1234567890"))
    assert re.fullmatch(
        r"proc_seguros_202604_[0-9a-f]{12}",
        new_process_id("seguros", "202604", lambda: "123456abcdef7890"),
    )
    assert validate_data_referencia("202604") == "202604"
    assert utc_now_iso(lambda: "2026-04-30T12:34:56+00:00") == "2026-04-30T12:34:56+00:00"

    with pytest.raises(ValueError):
        validate_data_referencia("2026-04")


def test_glue_catalog_ingestion_lifecycle_updates_current_sor_only_on_success():
    control = InMemoryControlPlane()

    prepared = prepare_ingestion(
        {
            "database_name": "raw",
            "table_name": "contratos",
            "data_referencia": "202604",
            "source_type": "glue_catalog",
            "partition_values": {"ano_mes": "202604"},
        },
        None,
        control_plane=control,
        id_factory=lambda: "aaaabbbbccccdddd",
        clock=lambda: "2026-04-30T12:00:00+00:00",
    )

    assert prepared["ingestion_id"] == "ing_aaaabbbbcccc"
    metadata = control.get_ingestion_metadata("ing_aaaabbbbcccc")
    assert metadata["status"] == "PROCESSING"
    assert metadata["source_type"] == "glue_catalog"
    assert "process_name" not in metadata

    plan = resolve_sor_load_plan("ing_aaaabbbbcccc", control)
    assert plan["read_mode"] == "glue_catalog"
    assert plan["arguments"] == {"INGESTION_ID": "ing_aaaabbbbcccc"}
    assert build_catalog_pushdown_predicate(plan["metadata"]) == "ano_mes='202604'"

    failed = fail_ingestion(
        {"ingestion_id": "ing_aaaabbbbcccc", "error": "boom"},
        None,
        control_plane=control,
        clock=lambda: "2026-04-30T12:05:00+00:00",
    )
    assert failed["status"] == "FAILED"
    assert control.get_current_sor("contratos", "202604") is None

    succeed_ingestion(
        {"ingestion_id": "ing_aaaabbbbcccc"},
        None,
        control_plane=control,
        clock=lambda: "2026-04-30T12:10:00+00:00",
    )
    current = control.get_current_sor("contratos", "202604")
    assert current["ingestion_id"] == "ing_aaaabbbbcccc"
    assert control.get_ingestion_metadata("ing_aaaabbbbcccc")["status"] == "SOR_LOADED"


def test_csv_s3_ingestion_uses_same_generic_contract_without_process_name():
    control = InMemoryControlPlane()

    prepared = prepare_ingestion(
        {
            "table_name": "clientes",
            "data_referencia": "202604",
            "source_type": "csv_s3",
            "s3_path": "s3://landing/clientes/202604/clientes.csv",
            "csv_options": {"header": True, "sep": ";"},
        },
        None,
        control_plane=control,
        id_factory=lambda: "1111222233334444",
        clock=lambda: "2026-04-30T12:00:00+00:00",
    )

    metadata = control.get_ingestion_metadata(prepared["ingestion_id"])
    plan = resolve_sor_load_plan(prepared["ingestion_id"], control)

    assert metadata["source_type"] == "csv_s3"
    assert "process_name" not in metadata
    assert plan["read_mode"] == "csv_s3"
    assert plan["source"]["s3_path"] == "s3://landing/clientes/202604/clientes.csv"


def test_full_preparation_creates_immutable_input_locks_and_reports_missing_inputs():
    control = InMemoryControlPlane()
    for table_name in ("contratos", "clientes", "parcelas"):
        control.put_full_config_table("seguros", table_name)
        control.update_current_sor(
            table_name,
            "202604",
            {"ingestion_id": f"ing_{table_name}", "loaded_at": "2026-04-30T12:00:00+00:00"},
        )

    result = prepare_full(
        {"process_name": "seguros", "data_referencia": "202604"},
        None,
        control_plane=control,
        id_factory=lambda: "abcabcabcabc9999",
        clock=lambda: "2026-04-30T13:00:00+00:00",
    )

    assert result["status"] == "READY"
    assert result["process_id"] == "proc_seguros_202604_abcabcabcabc"
    assert control.get_process_header(result["process_id"])["status"] == "READY"
    assert control.get_process_list("seguros", "202604")[0]["process_id"] == result["process_id"]
    assert control.get_input_lock(result["process_id"], "contratos")["ingestion_id"] == "ing_contratos"

    with pytest.raises(DuplicateItemError):
        control.create_input_lock(
            result["process_id"],
            "contratos",
            {"ingestion_id": "other"},
        )

    missing_control = InMemoryControlPlane()
    missing_control.put_full_config_table("seguros", "contratos")
    missing = prepare_full(
        {"process_name": "seguros", "data_referencia": "202604"},
        None,
        control_plane=missing_control,
        id_factory=lambda: "ffffeeee00001111",
        clock=lambda: "2026-04-30T13:00:00+00:00",
    )
    assert missing["status"] == "FAILED_INPUT_MISSING"
    assert missing["missing_tables"] == ["contratos"]
    assert missing_control.list_input_locks(missing["process_id"]) == []


def test_process_module_resolves_process_id_to_ingestion_predicates_without_data_referencia():
    control = InMemoryControlPlane()
    process_id = "proc_seguros_202604_deadbeef0000"
    control.create_input_lock(process_id, "contratos", {"ingestion_id": "ing_contracts"})
    control.create_input_lock(process_id, "clientes", {"ingestion_id": "ing_clients"})

    sources = resolve_locked_sources(process_id, ["contratos", "clientes"], control)

    assert sources["contratos"]["predicate"] == "ingestion_id='ing_contracts'"
    assert sources["clientes"]["predicate"] == "ingestion_id='ing_clients'"
    assert build_ingestion_predicate("ing_contracts") == "ingestion_id='ing_contracts'"
    assert "data_referencia" not in json.dumps(sources)


def test_step_function_passes_only_process_id_to_modules_and_tracks_later_steps():
    definition = json.loads(Path("infra/statemachines/full_seguros.asl.json").read_text())
    states = definition["States"]

    assert states["PrepararFull"]["Parameters"]["Payload"]["process_name"] == "seguros"
    for name in ("ModuloX", "ModuloY", "ModuloZ", "ConsolidacaoSot", "PublicacaoSpec"):
        args = states[name]["Parameters"]["Arguments"]
        assert args == {"--PROCESS_ID.$": "$.process_id"}

    control = InMemoryControlPlane()
    update_step_status(
        {
            "process_id": "proc_seguros_202604_deadbeef0000",
            "step_name": "PUBLICATION",
            "status": "SUCCEEDED",
            "process_status": "PUBLISHED",
        },
        None,
        control_plane=control,
        clock=lambda: "2026-04-30T14:00:00+00:00",
    )
    assert control.get_step_status("proc_seguros_202604_deadbeef0000", "PUBLICATION")["status"] == "SUCCEEDED"
    assert control.get_process_header("proc_seguros_202604_deadbeef0000")["status"] == "PUBLISHED"

