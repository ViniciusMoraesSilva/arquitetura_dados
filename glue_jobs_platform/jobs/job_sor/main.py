"""Job fino SOR."""

from __future__ import annotations

from pathlib import Path

import boto3

from glue_jobs_lib import GlueArguments, GlueJobRunner, SorPlanResolver

BASE_PATH = Path(__file__).resolve().parent
AWS_REGION = "us-east-2"


def main() -> None:
    """Executa o job SOR."""
    runner = GlueJobRunner.from_aws()
    resolver = SorPlanResolver(
        source_config_path=BASE_PATH / "config" / "config_origem_dados.json",
        destination_config_path=BASE_PATH / "config" / "config_destino_dados.json",
        sql_base_path=BASE_PATH,
        arguments=GlueArguments(),
        dynamodb_resource=boto3.resource("dynamodb", region_name=AWS_REGION),
    )
    runner.run(resolver)


if __name__ == "__main__":
    main()
