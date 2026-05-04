output "control_table_name" {
  value = aws_dynamodb_table.pipeline_control.name
}

output "pipeline_bucket" {
  value = aws_s3_bucket.pipeline.bucket
}

output "full_seguros_state_machine_arn" {
  value = aws_sfn_state_machine.full_seguros.arn
}

