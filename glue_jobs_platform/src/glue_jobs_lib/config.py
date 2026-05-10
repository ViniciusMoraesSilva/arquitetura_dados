"""Leitura e selecao de configuracoes JSON."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def load_json(path: str | Path) -> dict[str, Any]:
    """Carrega JSON de config."""
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Arquivo de configuracao nao encontrado: {config_path}")
    return json.loads(config_path.read_text(encoding="utf-8"))


def select_one(
    items: list[dict[str, Any]],
    filters: dict[str, str],
    description: str,
) -> dict[str, Any]:
    """Seleciona exatamente um item por filtros simples."""
    matches = [
        item for item in items if all(item.get(key) == value for key, value in filters.items())
    ]
    if not matches:
        filters_text = ", ".join(f"{key}={value}" for key, value in filters.items())
        raise ValueError(f"{description} nao encontrado para {filters_text}.")
    if len(matches) > 1:
        raise ValueError(f"Mais de um {description} encontrado.")
    return matches[0]
