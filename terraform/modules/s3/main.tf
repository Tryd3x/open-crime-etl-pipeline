resource "aws_s3_bucket" "open-crime-etl-pipeline" {
  bucket = var.bucket_name
  force_destroy = true
}