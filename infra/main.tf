provider "aws" {
  region = var.aws_region
}

locals {
  name_prefix        = "${var.project_name}-${var.environment}"
  bucket_name        = "${var.project_name}-${var.environment}-mvp1"
  control_table_name = "pipeline_control_${var.environment}"
  jobglue_asset_root = "../src/glue/jobglue_process_module"
  jobglue_assets     = fileset("${path.module}/${local.jobglue_asset_root}", "**")
  module_jobs = {
    x             = "x/config.json"
    y             = "y/config.json"
    z             = "z/config.json"
    consolidation = "consolidation/config.json"
    publication   = "publication/config.json"
  }
}

data "archive_file" "lambda_src" {
  type        = "zip"
  source_dir  = "${path.module}/../src"
  output_path = "${path.module}/.terraform/build/lambda-src.zip"
}

resource "aws_s3_bucket" "pipeline" {
  bucket = local.bucket_name
}

resource "aws_s3_object" "prefixes" {
  for_each = toset(["sor/", "sot/", "spec/", "scripts/", "tmp/"])

  bucket  = aws_s3_bucket.pipeline.id
  key     = each.value
  content = ""
}

resource "aws_s3_object" "jobglue_assets" {
  for_each = local.jobglue_assets

  bucket = aws_s3_bucket.pipeline.id
  key    = "scripts/jobglue/${each.value}"
  source = "${path.module}/${local.jobglue_asset_root}/${each.value}"
  etag   = filemd5("${path.module}/${local.jobglue_asset_root}/${each.value}")
}

resource "aws_s3_object" "module_configs" {
  for_each = local.module_jobs

  bucket = aws_s3_bucket.pipeline.id
  key    = "scripts/modulos_seguros/${each.value}"
  source = "${path.module}/../src/glue/modulos_seguros/${each.value}"
  etag   = filemd5("${path.module}/../src/glue/modulos_seguros/${each.value}")
}

resource "aws_dynamodb_table" "pipeline_control" {
  name         = local.control_table_name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "PK"
  range_key    = "SK"

  attribute {
    name = "PK"
    type = "S"
  }

  attribute {
    name = "SK"
    type = "S"
  }
}

resource "aws_dynamodb_table_item" "full_config_seguros" {
  for_each = toset(var.full_config_tables)

  table_name = aws_dynamodb_table.pipeline_control.name
  hash_key   = aws_dynamodb_table.pipeline_control.hash_key
  range_key  = aws_dynamodb_table.pipeline_control.range_key

  item = jsonencode({
    PK = { S = "FULL_CONFIG#seguros" }
    SK = { S = "TABLE#${each.value}" }
    process_name = { S = "seguros" }
    table_name   = { S = each.value }
  })
}

resource "aws_glue_catalog_database" "sor" {
  name = replace("${local.name_prefix}_sor", "-", "_")
}

resource "aws_glue_catalog_database" "sot" {
  name = replace("${local.name_prefix}_sot", "-", "_")
}

resource "aws_glue_catalog_database" "spec" {
  name = replace("${local.name_prefix}_spec", "-", "_")
}

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "glue_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "states_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda" {
  name               = "${local.name_prefix}-lambda-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

resource "aws_iam_role" "glue" {
  name               = "${local.name_prefix}-glue-role"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
}

resource "aws_iam_role" "states" {
  name               = "${local.name_prefix}-states-role"
  assume_role_policy = data.aws_iam_policy_document.states_assume.json
}

resource "aws_iam_role_policy" "lambda" {
  name = "${local.name_prefix}-lambda-policy"
  role = aws_iam_role.lambda.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["dynamodb:*"]
        Resource = [aws_dynamodb_table.pipeline_control.arn]
      },
      {
        Effect = "Allow"
        Action = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy" "glue" {
  name = "${local.name_prefix}-glue-policy"
  role = aws_iam_role.glue.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["dynamodb:*", "glue:*"]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = ["s3:*"]
        Resource = [aws_s3_bucket.pipeline.arn, "${aws_s3_bucket.pipeline.arn}/*"]
      },
      {
        Effect = "Allow"
        Action = ["logs:*"]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy" "states" {
  name = "${local.name_prefix}-states-policy"
  role = aws_iam_role.states.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["lambda:InvokeFunction", "glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"]
        Resource = "*"
      }
    ]
  })
}

resource "aws_lambda_function" "preparar_ingestao" {
  function_name    = "${local.name_prefix}-preparar-ingestao"
  filename         = data.archive_file.lambda_src.output_path
  source_code_hash = data.archive_file.lambda_src.output_base64sha256
  role             = aws_iam_role.lambda.arn
  handler          = "lambdas.preparar_ingestao.handler"
  runtime          = "python3.12"
  timeout          = 60

  environment {
    variables = {
      CONTROL_TABLE_NAME = aws_dynamodb_table.pipeline_control.name
    }
  }
}

resource "aws_lambda_function" "preparar_full" {
  function_name    = "${local.name_prefix}-preparar-full"
  filename         = data.archive_file.lambda_src.output_path
  source_code_hash = data.archive_file.lambda_src.output_base64sha256
  role             = aws_iam_role.lambda.arn
  handler          = "lambdas.preparar_full_generica.handler"
  runtime          = "python3.12"
  timeout          = 60

  environment {
    variables = {
      CONTROL_TABLE_NAME = aws_dynamodb_table.pipeline_control.name
    }
  }
}

resource "aws_lambda_function" "atualizar_step_status" {
  function_name    = "${local.name_prefix}-atualizar-step-status"
  filename         = data.archive_file.lambda_src.output_path
  source_code_hash = data.archive_file.lambda_src.output_base64sha256
  role             = aws_iam_role.lambda.arn
  handler          = "lambdas.atualizar_step_status.handler"
  runtime          = "python3.12"
  timeout          = 60

  environment {
    variables = {
      CONTROL_TABLE_NAME = aws_dynamodb_table.pipeline_control.name
    }
  }
}

resource "aws_glue_job" "carga_sor_generica" {
  name     = "${local.name_prefix}-carga-sor-generica"
  role_arn = aws_iam_role.glue.arn
  depends_on = [
    aws_s3_object.jobglue_assets
  ]

  command {
    script_location = "s3://${aws_s3_bucket.pipeline.bucket}/scripts/jobglue/main.py"
    python_version  = "3"
  }

  default_arguments = {
    "--CONTROL_TABLE_NAME" = aws_dynamodb_table.pipeline_control.name
    "--JOB_MODE"           = "sor"
    "--GLUE_SQL_LOCATION"  = "s3://${aws_s3_bucket.pipeline.bucket}/scripts/jobglue"
    "--AMBIENTE"           = var.environment
  }
}

resource "aws_glue_job" "modulos_seguros" {
  for_each = local.module_jobs

  name     = "${local.name_prefix}-seguros-${each.key}"
  role_arn = aws_iam_role.glue.arn
  depends_on = [
    aws_s3_object.jobglue_assets,
    aws_s3_object.module_configs
  ]

  command {
    script_location = "s3://${aws_s3_bucket.pipeline.bucket}/scripts/jobglue/main.py"
    python_version  = "3"
  }

  default_arguments = {
    "--CONTROL_TABLE_NAME" = aws_dynamodb_table.pipeline_control.name
    "--JOB_MODE"           = "process"
    "--GLUE_SQL_LOCATION"  = "s3://${aws_s3_bucket.pipeline.bucket}/scripts/jobglue"
    "--AMBIENTE"           = var.environment
    "--MODULE_CONFIG"      = "s3://${aws_s3_bucket.pipeline.bucket}/${aws_s3_object.module_configs[each.key].key}"
  }
}

resource "aws_sfn_state_machine" "full_seguros" {
  name     = "${local.name_prefix}-full-seguros"
  role_arn = aws_iam_role.states.arn
  definition = templatefile("${path.module}/statemachines/full_seguros.asl.json", {
    prepare_full_lambda_arn       = aws_lambda_function.preparar_full.arn
    update_step_status_lambda_arn = aws_lambda_function.atualizar_step_status.arn
    module_x_job_name             = aws_glue_job.modulos_seguros["x"].name
    module_y_job_name             = aws_glue_job.modulos_seguros["y"].name
    module_z_job_name             = aws_glue_job.modulos_seguros["z"].name
    consolidation_job_name        = aws_glue_job.modulos_seguros["consolidation"].name
    publication_job_name          = aws_glue_job.modulos_seguros["publication"].name
  })
}
