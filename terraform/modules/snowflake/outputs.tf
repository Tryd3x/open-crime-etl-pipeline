output "warehouse_name" {
  value = snowflake_warehouse.crime_wh.name
  description = "Warehouse Name"
}

output "database_name" {
  value = snowflake_database.crime_db.name
  description = "Database Name"
}

