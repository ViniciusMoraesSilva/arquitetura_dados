"""Resolucao generica de templates usados nas configuracoes do job SOR."""

from __future__ import annotations

import re
from string import Formatter
from typing import Any


def normalizar_nome_placeholder(valor: str) -> str:
    """Converte nomes de particao para o padrao de placeholder."""
    return re.sub(r"[^A-Z0-9_]", "_", valor.upper())


def resolver_template(campo: str, valor: Any, contexto: dict[str, str]) -> Any:
    """Resolve um valor textual e falha quando houver placeholders desconhecidos."""
    if not isinstance(valor, str):
        return valor

    placeholders = [
        nome_campo
        for _, nome_campo, _, _ in Formatter().parse(valor)
        if nome_campo is not None
    ]
    desconhecidos = [
        placeholder for placeholder in placeholders if placeholder not in contexto
    ]
    if desconhecidos:
        raise ValueError(
            f"Placeholder desconhecido na configuracao para o campo {campo}: "
            + ", ".join(desconhecidos)
        )

    return valor.format(**contexto)
