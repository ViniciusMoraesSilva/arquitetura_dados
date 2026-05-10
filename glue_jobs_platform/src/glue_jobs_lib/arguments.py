"""Resolucao de argumentos Glue com contrato por dominio."""

from __future__ import annotations

import sys
from collections.abc import Callable

GetOptions = Callable[[list[str], list[str]], dict[str, str]]


class GlueArguments:
    """Resolve argumentos obrigatorios e opcionais de forma testavel."""

    def __init__(
        self,
        get_options: GetOptions | None = None,
        argv: list[str] | None = None,
    ) -> None:
        self.argv = argv or sys.argv
        if get_options is None:
            from awsglue.utils import getResolvedOptions

            self.get_options = getResolvedOptions
        else:
            self.get_options = get_options

    def resolve_required(self, names: list[str]) -> dict[str, str]:
        """Resolve argumentos obrigatorios."""
        return self.get_options(self.argv, names)

    def resolve_optional(self, name: str) -> str | None:
        """Resolve um argumento opcional."""
        try:
            return self.get_options(self.argv, [name])[name]
        except Exception:
            return None

    def resolve_sot(self) -> dict[str, str]:
        """Resolve argumentos obrigatorios do SOT."""
        return self.resolve_required(
            ["JOB_NAME", "PROCESS_ID", "DATA_REF", "INGESTION_ID", "AMBIENTE", "DBLOCAL"]
        )

    def resolve_spec(self) -> dict[str, str]:
        """Resolve argumentos obrigatorios do SPEC."""
        return self.resolve_required(
            ["JOB_NAME", "PROCESS_ID", "DATA_REF", "AMBIENTE", "DBLOCAL"]
        )

    def resolve_sor(self) -> dict[str, str]:
        """Resolve argumentos obrigatorios do SOR."""
        return self.resolve_required(["JOB_NAME", "INGESTION_ID", "AMBIENTE", "DBLOCAL"])
