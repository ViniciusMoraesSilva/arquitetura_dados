"""Modelos internos compartilhados pelos jobs Glue."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class SourceConfig:
    """Fonte de dados a ser carregada como temp view."""

    name: str
    source_type: str
    view_name: str
    options: dict[str, Any]
    predicate: str | None = None
    duplicate_fields: list[str] | None = None


@dataclass(frozen=True)
class DestinationConfig:
    """Destino final da execucao."""

    output_type: str
    options: dict[str, Any]


@dataclass(frozen=True)
class ExecutionPlan:
    """Plano comum usado pelo runner."""

    domain: str
    execution_key: str
    args: dict[str, str]
    context: dict[str, Any]
    sources: list[SourceConfig]
    sql_path: str
    destination: DestinationConfig
    post_columns: dict[str, str] = field(default_factory=dict)
    metadata: dict[str, Any] | None = None
