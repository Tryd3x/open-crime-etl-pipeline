locals {
    # S3
    region = "us-east-2"

    # Snowflake
    warehouse_name = "crime_wh"
    wh_description = "This warehouse compute is used as a dev environment to test the crime ETL"
    db_name = "crime_db"
    db_description = "This database holds information related to crime etl"
}