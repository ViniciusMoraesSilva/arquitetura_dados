"""Stubs mínimos de dependências Glue e PySpark para testes unitários."""

from __future__ import annotations

import sys
import types


def install_dependency_stubs() -> None:
    """Registra módulos fake de Glue e PySpark quando eles não existirem."""
    if "boto3" not in sys.modules:
        boto3_module = types.ModuleType("boto3")
        boto3_module.client = lambda *args, **kwargs: None
        boto3_module.resource = lambda *args, **kwargs: None
        sys.modules["boto3"] = boto3_module

    if "awsglue" not in sys.modules:
        awsglue_module = types.ModuleType("awsglue")
        awsglue_context = types.ModuleType("awsglue.context")
        awsglue_job = types.ModuleType("awsglue.job")
        awsglue_utils = types.ModuleType("awsglue.utils")
        awsglue_dynamicframe = types.ModuleType("awsglue.dynamicframe")

        class GlueContext:  # pragma: no cover - stub simples
            def __init__(self, sc):
                self.spark_session = None

        class Job:  # pragma: no cover - stub simples
            def __init__(self, glue_context):
                self.glue_context = glue_context

            def init(self, *args, **kwargs):
                return None

        class GlueArgumentError(Exception):
            """Exceção fake compatível com awsglue.utils."""

        class DynamicFrame:  # pragma: no cover - stub simples
            @staticmethod
            def fromDF(df, glue_context, name):
                return {
                    "df": df,
                    "glue_context": glue_context,
                    "name": name,
                }

        def get_resolved_options(*args, **kwargs):
            raise GlueArgumentError("stub")

        awsglue_context.GlueContext = GlueContext
        awsglue_job.Job = Job
        awsglue_utils.GlueArgumentError = GlueArgumentError
        awsglue_utils.getResolvedOptions = get_resolved_options
        awsglue_dynamicframe.DynamicFrame = DynamicFrame

        sys.modules["awsglue"] = awsglue_module
        sys.modules["awsglue.context"] = awsglue_context
        sys.modules["awsglue.job"] = awsglue_job
        sys.modules["awsglue.utils"] = awsglue_utils
        sys.modules["awsglue.dynamicframe"] = awsglue_dynamicframe

    if "pyspark" not in sys.modules:
        pyspark_module = types.ModuleType("pyspark")
        pyspark_context = types.ModuleType("pyspark.context")
        pyspark_sql = types.ModuleType("pyspark.sql")
        pyspark_sql_session = types.ModuleType("pyspark.sql.session")
        pyspark_sql_functions = types.ModuleType("pyspark.sql.functions")

        class SparkContext:  # pragma: no cover - stub simples
            pass

        class SparkSession:  # pragma: no cover - stub simples
            pass

        class DataFrame:  # pragma: no cover - stub simples
            pass

        def monotonically_increasing_id():
            return "monotonically_increasing_id"

        pyspark_context.SparkContext = SparkContext
        pyspark_sql.DataFrame = DataFrame
        pyspark_sql.SparkSession = SparkSession
        pyspark_sql_session.SparkSession = SparkSession
        pyspark_sql_functions.monotonically_increasing_id = monotonically_increasing_id

        sys.modules["pyspark"] = pyspark_module
        sys.modules["pyspark.context"] = pyspark_context
        sys.modules["pyspark.sql"] = pyspark_sql
        sys.modules["pyspark.sql.session"] = pyspark_sql_session
        sys.modules["pyspark.sql.functions"] = pyspark_sql_functions
