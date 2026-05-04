variable "environment" {
  type        = string
  description = "Deployment environment."
  default     = "dev"
}

variable "aws_region" {
  type        = string
  description = "AWS region for the MVP1 resources."
  default     = "us-east-2"
}

variable "project_name" {
  type        = string
  description = "Project name used as a resource-name prefix."
  default     = "arquitetura-dados"
}

variable "full_config_tables" {
  type        = list(string)
  description = "Initial SOR table placeholders required by FULL seguros."
  default     = ["contratos", "clientes", "parcelas"]
}

