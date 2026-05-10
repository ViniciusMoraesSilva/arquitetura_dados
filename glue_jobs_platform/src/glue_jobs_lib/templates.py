"""Resolucao segura de templates em configs."""

from __future__ import annotations

from string import Formatter
from typing import Any


def template_uses(value: Any, placeholder: str) -> bool:
    """Indica se uma estrutura usa um placeholder."""
    if isinstance(value, dict):
        return any(template_uses(item, placeholder) for item in value.values())
    if isinstance(value, list):
        return any(template_uses(item, placeholder) for item in value)
    if not isinstance(value, str):
        return False
    return placeholder in {
        name for _, name, _, _ in Formatter().parse(value) if name is not None
    }


def render_value(field: str, value: Any, context: dict[str, str]) -> Any:
    """Renderiza strings e falha para placeholders desconhecidos."""
    if isinstance(value, dict):
        return {key: render_value(key, item, context) for key, item in value.items()}
    if isinstance(value, list):
        return [render_value(field, item, context) for item in value]
    if not isinstance(value, str):
        return value

    placeholders = [
        name for _, name, _, _ in Formatter().parse(value) if name is not None
    ]
    unknown = [name for name in placeholders if name not in context]
    if unknown:
        raise ValueError(
            f"Placeholder desconhecido no campo {field}: " + ", ".join(unknown)
        )
    return value.format(**context)


def render_config(config: dict[str, Any], context: dict[str, str]) -> dict[str, Any]:
    """Renderiza uma config inteira."""
    return render_value("config", config, context)
