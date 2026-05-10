"""Resolvers de plano para SOT, SPEC e SOR."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import re
from typing import Any

from .arguments import GlueArguments
from .config import load_json, select_one
from .metadata import DEFAULT_PIPELINE_CONTROL_TABLE, get_sor_metadata, get_spec_ingestion_id
from .models import DestinationConfig, ExecutionPlan, SourceConfig
from .predicates import get_source_name, parse_table_predicates, resolve_predicate
from .templates import render_config, template_uses


def _date_context(data_ref: str) -> dict[str, str]:
    if len(data_ref) == 6:
        date_ref = datetime.strptime(data_ref, "%Y%m")
        ano_mes = data_ref
    elif len(data_ref) == 8:
        date_ref = datetime.strptime(data_ref, "%Y%m%d")
        ano_mes = data_ref[:6]
    else:
        raise ValueError("DATA_REF invalida. Use YYYYMM ou YYYYMMDD.")

    first_day = date_ref.replace(day=1)
    if first_day.month == 1:
        previous = f"{first_day.year - 1}12"
    else:
        previous = f"{first_day.year}{first_day.month - 1:02d}"

    return {
        "DATA_REF": data_ref,
        "DATA_ATUAL": datetime.now().strftime("%Y%m%d"),
        "ANO_DATA_REF": str(date_ref.year),
        "MES_DATA_REF": f"{date_ref.month:02d}",
        "ANO_MES_DATA_REF": ano_mes,
        "MES_ANTERIOR": previous[-2:],
        "ANO_MES_ANTERIOR": previous,
    }


def _normalize_placeholder_name(value: str) -> str:
    return re.sub(r"[^A-Z0-9_]", "_", value.upper())


def _read_options(source_config: dict[str, Any]) -> dict[str, Any]:
    catalog = source_config.get("catalogo", {})
    return {
        "group_files": catalog["group_files"],
        "group_size_bytes": catalog["group_size_bytes"],
    }


def _destination_options(
    destination_config: dict[str, Any],
    raw_destination: dict[str, Any],
    args: dict[str, str],
) -> DestinationConfig:
    output_type = raw_destination.get("tipo_saida_destino", "catalogo").strip().lower()
    env_options = destination_config["ambientes"][args["AMBIENTE"]]
    if output_type == "catalogo":
        options = {
            **destination_config["catalogo"],
            **env_options,
            **raw_destination,
        }
    elif output_type == "csv":
        options = {
            **destination_config.get("csv", {}),
            **env_options,
            **raw_destination,
        }
    else:
        raise ValueError("tipo_saida_destino invalido. Use catalogo ou csv.")
    return DestinationConfig(output_type=output_type, options=options)


def _source_configs(
    raw_sources: list[dict[str, Any]],
    argument_predicates: dict[str, str],
    ddb_predicates: dict[str, str] | None = None,
) -> list[SourceConfig]:
    sources = []
    for raw_source in raw_sources:
        source_name = get_source_name(raw_source)
        source_type = raw_source.get("tipo_origem", "catalogo").strip().lower()
        predicate = resolve_predicate(
            source_name,
            raw_source.get("filtro_origem"),
            argument_predicates,
            ddb_predicates,
        )
        sources.append(
            SourceConfig(
                name=source_name,
                source_type=source_type,
                view_name=raw_source.get("nome_view") or source_name,
                options=raw_source,
                predicate=predicate,
                duplicate_fields=raw_source.get("campos_duplicidade"),
            )
        )
    return sources


@dataclass
class BaseResolver:
    """Base comum para resolvers."""

    source_config_path: str | Path
    destination_config_path: str | Path
    sql_base_path: str | Path
    arguments: GlueArguments
    dynamodb_resource: Any | None = None
    pipeline_control_table: str = DEFAULT_PIPELINE_CONTROL_TABLE

    def _optional_predicates(self) -> dict[str, str]:
        return parse_table_predicates(self.arguments.resolve_optional("TABLE_PREDICATES"))

    def _sql_path(self, raw_path: str) -> str:
        path = Path(raw_path)
        if not path.is_absolute():
            path = Path(self.sql_base_path) / path
        return str(path)


class SotPlanResolver(BaseResolver):
    """Resolve plano SOT por PROCESS_ID, DATA_REF e INGESTION_ID."""

    def resolve(self) -> ExecutionPlan:
        args = self.arguments.resolve_sot()
        source_config = load_json(self.source_config_path)
        destination_config = load_json(self.destination_config_path)
        context = {
            "PROCESS_ID": args["PROCESS_ID"],
            "INGESTION_ID": args["INGESTION_ID"],
            **_date_context(args["DATA_REF"]),
        }
        raw_execution = select_one(
            source_config.get("execucoes", []),
            {"domain": "sot", "process_id": args["PROCESS_ID"]},
            "execucao SOT",
        )
        execution = render_config(raw_execution, context)
        raw_destination = select_one(
            destination_config.get("destinos", []),
            {"domain": "sot", "process_id": args["PROCESS_ID"]},
            "destino SOT",
        )
        context["_read_options"] = _read_options(source_config)  # type: ignore[assignment]
        return ExecutionPlan(
            domain="sot",
            execution_key=args["PROCESS_ID"],
            args=args,
            context=context,
            sources=_source_configs(execution["fontes"], self._optional_predicates()),
            sql_path=self._sql_path(execution["sql_path"]),
            destination=_destination_options(destination_config, raw_destination, args),
            post_columns=execution.get("post_columns", {"process_id": args["PROCESS_ID"]}),
        )


class SpecPlanResolver(BaseResolver):
    """Resolve plano SPEC por PROCESS_ID e DATA_REF."""

    def resolve(self) -> ExecutionPlan:
        args = self.arguments.resolve_spec()
        source_config = load_json(self.source_config_path)
        destination_config = load_json(self.destination_config_path)
        raw_execution = select_one(
            source_config.get("execucoes", []),
            {"domain": "spec", "process_id": args["PROCESS_ID"]},
            "execucao SPEC",
        )
        context = {"PROCESS_ID": args["PROCESS_ID"], **_date_context(args["DATA_REF"])}
        if template_uses(raw_execution, "INGESTION_ID"):
            if self.dynamodb_resource is None:
                raise ValueError("SPEC exige dynamodb_resource para resolver INGESTION_ID.")
            context["INGESTION_ID"] = get_spec_ingestion_id(
                self.dynamodb_resource,
                args["PROCESS_ID"],
                args["DATA_REF"],
                self.pipeline_control_table,
            )

        execution = render_config(raw_execution, context)
        destinations = [
            item
            for item in destination_config.get("destinos", [])
            if item.get("domain") == "spec" and item.get("process_id") == args["PROCESS_ID"]
        ]
        if len(destinations) != 1:
            raise ValueError("SPEC exige exatamente um destino final configurado.")

        context["_read_options"] = _read_options(source_config)  # type: ignore[assignment]
        return ExecutionPlan(
            domain="spec",
            execution_key=args["PROCESS_ID"],
            args=args,
            context=context,
            sources=_source_configs(execution["fontes"], self._optional_predicates()),
            sql_path=self._sql_path(execution["sql_path"]),
            destination=_destination_options(destination_config, destinations[0], args),
            post_columns=execution.get("post_columns", {"process_id": args["PROCESS_ID"]}),
        )


class SorPlanResolver(BaseResolver):
    """Resolve plano SOR por INGESTION_ID e metadata DynamoDB."""

    def resolve(self) -> ExecutionPlan:
        args = self.arguments.resolve_sor()
        if self.dynamodb_resource is None:
            raise ValueError("SOR exige dynamodb_resource.")

        metadata = get_sor_metadata(
            self.dynamodb_resource,
            args["INGESTION_ID"],
            self.pipeline_control_table,
        )
        context = {
            "INGESTION_ID": str(metadata["ingestion_id"]),
            "PROCESS_NAME": str(metadata["process_name"]),
            "SOURCE_DATABASE_NAME": str(metadata["source_database_name"]),
            "SOURCE_TABLE_NAME": str(metadata["source_table_name"]),
            "SOR_DATABASE_NAME": str(metadata["sor_database_name"]),
            "SOR_TABLE_NAME": str(metadata["sor_table_name"]),
        }
        for key, value in metadata.get("partitions", {}).items():
            context[f"PARTITION_{_normalize_placeholder_name(str(key))}"] = str(value)

        source_config = load_json(self.source_config_path)
        destination_config = load_json(self.destination_config_path)
        raw_execution = select_one(
            source_config.get("execucoes", []),
            {
                "domain": "sor",
                "process_name": metadata["process_name"],
                "sor_table_name": metadata["sor_table_name"],
            },
            "execucao SOR",
        )
        execution = render_config(raw_execution, context)
        raw_destination = select_one(
            destination_config.get("destinos", []),
            {
                "domain": "sor",
                "process_name": metadata["process_name"],
                "sor_table_name": metadata["sor_table_name"],
            },
            "destino SOR",
        )

        context["_read_options"] = _read_options(source_config)  # type: ignore[assignment]
        return ExecutionPlan(
            domain="sor",
            execution_key=args["INGESTION_ID"],
            args=args,
            context=context,
            sources=_source_configs(execution["fontes"], self._optional_predicates()),
            sql_path=self._sql_path(execution["sql_path"]),
            destination=_destination_options(destination_config, raw_destination, args),
            post_columns=execution.get("post_columns", {"ingestion_id": args["INGESTION_ID"]}),
            metadata=metadata,
        )
