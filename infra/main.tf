# Squelette Terraform — S3 (raw zone) + Glue Data Catalog + IAM pour l'ingestion.
# À adapter (backend state, région, nommage) avant apply.

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

variable "aws_region" {
  default = "eu-west-3"
}

variable "bucket_name" {
  default = "energy-pipeline-demo"
}

resource "aws_s3_bucket" "raw" {
  bucket = var.bucket_name
}

resource "aws_s3_bucket_lifecycle_configuration" "raw_lifecycle" {
  bucket = aws_s3_bucket.raw.id
  rule {
    id     = "archive-old-raw"
    status = "Enabled"
    transition {
      days          = 90
      storage_class = "GLACIER"
    }
  }
}

resource "aws_glue_catalog_database" "energy_raw" {
  name = "energy_raw"
}

resource "aws_glue_crawler" "smart_meters_crawler" {
  name          = "energy-smart-meters-crawler"
  role          = aws_iam_role.glue_role.arn
  database_name = aws_glue_catalog_database.energy_raw.name

  s3_target {
    path = "s3://${aws_s3_bucket.raw.bucket}/raw/smart-meters/"
  }

  schedule = "cron(0 2 * * ? *)"  # tous les jours à 2h
}

resource "aws_iam_role" "glue_role" {
  name = "energy-pipeline-glue-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

output "raw_bucket" {
  value = aws_s3_bucket.raw.bucket
}
