# Configure AWS credentials here, Currently using awscli for credentials
provider "aws" {
    region = local.region
}

# Configure Snowflake credentails here, Currently uses ~/.snowflake/config's `crime-etl` params
provider "snowflake" {
    profile = "crime_etl"
    password = var.snowflake_password
}

# Might add docker as a provider
provider "docker" {
    host = "unix:///var/run/docker.sock"
}