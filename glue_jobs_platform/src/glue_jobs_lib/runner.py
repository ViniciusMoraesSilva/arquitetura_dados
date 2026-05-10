"""Runner comum para jobs Glue."""

from __future__ import annotations

from typing import Any, Protocol

from .io import load_source, write_result
from .sql import execute_sql_file


class PlanResolver(Protocol):
    """Contrato de um resolver de plano."""

    def resolve(self) -> Any:
        """Monta e retorna um ExecutionPlan."""


class GlueJobRunner:
    """Executa um ExecutionPlan em Glue/Spark."""

    def __init__(
        self,
        glue_context: Any | None = None,
        spark: Any | None = None,
        job: Any | None = None,
    ) -> None:
        self.glue_context = glue_context
        self.spark = spark
        self.job = job

    @classmethod
    def from_aws(cls) -> "GlueJobRunner":
        """Inicializa SparkContext, GlueContext e Job dentro do ambiente Glue."""
        from awsglue.context import GlueContext
        from awsglue.job import Job
        from pyspark.context import SparkContext

        sc = SparkContext()
        glue_context = GlueContext(sc)
        return cls(glue_context=glue_context, spark=glue_context.spark_session, job=Job(glue_context))

    def run(self, resolver: PlanResolver) -> None:
        """Executa o plano resolvido."""
        plan = resolver.resolve()
        if self.job is not None:
            self.job.init(plan.args["JOB_NAME"], plan.args)

        read_options = plan.context.get("_read_options", {})
        for source in plan.sources:
            load_source(self.glue_context, source, read_options)

        df = execute_sql_file(self.spark, plan.sql_path)
        for column, value in plan.post_columns.items():
            from pyspark.sql.functions import lit

            df = df.withColumn(column, lit(value))

        write_result(self.glue_context, df, plan.destination)
