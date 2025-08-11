# Allocate Warehouse Compute
resource "snowflake_warehouse" "crime_wh" {
  name = var.warehouse_name
  comment = var.wh_description

  warehouse_size = local.wh_size
  auto_suspend = local.auto_suspend
  auto_resume = local.auto_resume
  initially_suspended = local.initially_suspended
}

# Create database
resource "snowflake_database" "crime_db" {
  name = var.db_name
  comment = var.db_description
}

# Tables are managed by airflow tasks
