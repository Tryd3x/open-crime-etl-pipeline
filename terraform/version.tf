terraform {
  required_version = ">= 1.12"

  required_providers {
    aws = {
        source = "hashicorp/aws"
        version = "6.4.0"
    }

    snowflake = {
      source = "snowflakedb/snowflake"
      version = "2.5.0"
    }

    docker = {
      source = "kreuzwerker/docker"
      version = "3.6.2"
    }
  }
}