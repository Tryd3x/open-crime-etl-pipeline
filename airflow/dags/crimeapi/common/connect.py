from sqlalchemy.engine.base import Engine
from sqlalchemy.engine.url import URL
from sqlalchemy import create_engine
import os
import boto3

def create_postgres_conn(
        host: str,
        port: str,
        username: str,
        password: str,
        db: str,
) -> Engine:
    """
    Connect to postgres Database
    """

    url = URL.create(
        drivername='postgresql',
        host=host,
        port=port,
        username=username,
        password=password,
        database=db
    )

    return create_engine(url)

def create_aws_conn(resource: str, access_key: str, secret_access_key: str, region: str):

    client = boto3.resource(
        resource,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_access_key,
        region_name=region
    )

    return client