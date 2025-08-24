FROM python:3.10-slim

# Grabs from build args passed during docker build in host machine
ARG dbt_core_version
ARG dbt_postgres_version
ARG dbt_snowflake_version

# Sets the environment variables inside the container
ENV DBT_CORE_VERSION=${dbt_core_version}
ENV DBT_POSTGRES_VERSION=${dbt_postgres_version}
ENV DBT_SNOWFLAKE_VERSION=${dbt_snowflake_version}

RUN apt-get update && \
    apt-get install -y \
        build-essential \
        git \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    dbt-core==${DBT_CORE_VERSION} \
    dbt-postgres==${DBT_POSTGRES_VERSION} \
    dbt-snowflake==${DBT_SNOWFLAKE_VERSION} \
    protobuf==4.23.3