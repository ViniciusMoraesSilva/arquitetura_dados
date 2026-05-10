"""Job fino SOT."""

from __future__ import annotations

from pathlib import Path

from glue_jobs_lib import GlueArguments, GlueJobRunner, SotPlanResolver

BASE_PATH = Path(__file__).resolve().parent


def main() -> None:
    """Executa o job SOT."""
    runner = GlueJobRunner.from_aws()
    resolver = SotPlanResolver(
        source_config_path=BASE_PATH / "config" / "config_origem_dados.json",
        destination_config_path=BASE_PATH / "config" / "config_destino_dados.json",
        sql_base_path=BASE_PATH,
        arguments=GlueArguments(),
    )
    runner.run(resolver)


if __name__ == "__main__":
    main()
