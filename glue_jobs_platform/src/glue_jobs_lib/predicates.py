"""Predicates e overrides."""

from __future__ import annotations

from typing import Any


def parse_table_predicates(value: str | None) -> dict[str, str]:
    """Parseia TABLE_PREDICATES no formato nome_fonte::predicate."""
    if not value:
        return {}

    result: dict[str, str] = {}
    for item in value.split(";"):
        item = item.strip()
        if not item or "::" not in item:
            continue
        name, predicate = item.split("::", 1)
        name = name.strip()
        predicate = predicate.strip()
        if name and predicate:
            result[name] = predicate
    return result


def get_source_name(raw_source: dict[str, Any]) -> str:
    """Resolve chave de predicate de uma fonte."""
    return str(
        raw_source.get("name")
        or raw_source.get("nome_fonte")
        or raw_source.get("source_table_name")
        or raw_source.get("tabela_origem")
        or raw_source.get("nome_view")
    )


def resolve_predicate(
    source_name: str,
    default_predicate: str | None,
    argument_overrides: dict[str, str] | None = None,
    ddb_overrides: dict[str, str] | None = None,
) -> str | None:
    """Resolve predicate final por prioridade argumento, DynamoDB e config."""
    argument_overrides = argument_overrides or {}
    ddb_overrides = ddb_overrides or {}
    if source_name in argument_overrides:
        return argument_overrides[source_name]
    if source_name in ddb_overrides:
        return ddb_overrides[source_name]
    return default_predicate
