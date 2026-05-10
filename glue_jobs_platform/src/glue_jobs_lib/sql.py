"""Execucao sequencial de SQL Spark."""

from __future__ import annotations

from pathlib import Path
from typing import Any


def split_sql(content: str) -> list[str]:
    """Separa consultas por ponto e virgula."""
    return [query.strip() for query in content.split(";") if query.strip()]


def execute_sql_file(
    spark: Any,
    sql_path: str | Path,
    temp_view_prefix: str = "query_temp_",
) -> Any:
    """Executa SQL sequencialmente e retorna o ultimo DataFrame."""
    path = Path(sql_path)
    if not path.exists():
        raise FileNotFoundError(f"Arquivo SQL nao encontrado: {path}")

    queries = split_sql(path.read_text(encoding="utf-8"))
    if not queries:
        raise ValueError("Arquivo SQL sem queries executaveis.")

    last_df = None
    for index, query in enumerate(queries, start=1):
        view_name = f"{temp_view_prefix}{index}"
        last_df = spark.sql(query)
        last_df.createOrReplaceTempView(view_name)

    return last_df
