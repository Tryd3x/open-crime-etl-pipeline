terraform {
  required_providers {
    aws = {
        source = "hashicorp/aws"
        version = "6.4.0"
    }
  }

  required_version = ">= 1.12"
}

module "open-crime-etl-bucket" {
  source = "./modules/s3"
  bucket_name = var.bucket_name
}