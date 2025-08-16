# resource "docker_image" "astro_airflow" {
#     name = "airflow_6a70b4/airflow:latest"
#     keep_locally = true   
# }

# resource "docker_container" "astro_airflow_container" {
#     name = "astro-airflow"
#     image = docker_image.astro_airflow.name
# }