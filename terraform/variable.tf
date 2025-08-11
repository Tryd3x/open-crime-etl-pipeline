# Provide variables that you would like to override using either:
# terraform apply -var="key=value"
# terraform apply -var-file="filename.tfvars"

variable "snowflake_password" {
  type = string
  sensitive = true
}