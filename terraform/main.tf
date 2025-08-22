module "crime_s3" {
  source = "./modules/s3"
  
  bucket_name = var.s3_bucket_name
}

module "crime_snowflake" {
  source = "./modules/snowflake"

  warehouse_name = local.warehouse_name
  wh_description = local.wh_description
  db_name = local.db_name
  db_description = local.db_description
}