module "open-crime-etl-bucket" {
  source = "./modules/s3"
  bucket_name = local.bucket_name
}