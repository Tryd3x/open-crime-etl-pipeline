output "warehouse_name" {
  value = module.crime_snowflake.warehouse_name
}

output "database_name" {
  value = module.crime_snowflake.database_name
}

output "s3_bucket_prefix" {
  value = module.crime_s3.bucket_name
}