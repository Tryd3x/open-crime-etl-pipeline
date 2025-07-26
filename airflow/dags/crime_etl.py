from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from dotenv import load_dotenv
load_dotenv()

import os
import json
import gzip
import boto3
import shutil
import requests
from pathlib import Path
from datetime import datetime, timedelta

import logging

logger = logging.getLogger("crime_etl")

default_args = {
    "retries" : 3,
    "retry_delay" : timedelta(seconds=15),
}

# Tasks Implementation
def save_to_path(path: Path, pagenum: int, data):
    # Iterates over each batch yielded by fetch_crime_data()
    filename = f"part-{pagenum:04}.json.gz"
    filepath = path / filename

    if not path.exists():
        logger.info(f"Missing {path.as_posix()}. Creating one...")
        path.mkdir(parents=True, exist_ok=True)

    # Staging batch in local storage for upload
    with gzip.open(filepath,'wt') as f:
        json.dump(data, f)
    logger.info(f"Saved file to: {filepath.as_posix()}")

def clear_dir(dir: Path):
    # Purge the temp folder
    logger.info(f"Clearing directory: {dir}")
    if dir.exists():
        for item in dir.iterdir():
            item.unlink()
            logger.info(f"Deleted: {item.as_posix()}")
        shutil.rmtree(dir)


def fetch_data_api(delta: int, pagesize: int, save_path: Path, **context):
    # Fetch data
    last_update = (datetime.now() - timedelta(days=delta)).isoformat()[:-3]

    query = f"SELECT * WHERE updated_on >= '{last_update}'"

    url = "https://data.cityofchicago.org/api/v3/views/crimes/query.json"
    headers = {'X-App-Token' : os.getenv('APP_TOKEN')}

    pagenum = 1
    while True:
        body = {
            'query' : query,
            'page' : {
                'pageNumber' : pagenum,
                'pageSize' : pagesize
            },
            "includeSynthetic": False
        }
        logger.info("Fetching from API")
        res = requests.post(
            url,
            json=body,
            headers=headers
        )

        if res.status_code != 200 or res.json() == [] or pagenum >= 50:
            break
        
        # Save data to local storage
        save_to_path(path=save_path, pagenum=pagenum, data=res.json())
        
        pagenum += 1

def upload_to_s3(bucket_name: str, filepath: Path, object_key: Path):
    # upload to s3
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    region_name = os.getenv("AWS_REGION")

    client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=region_name
    )

    logger.info(f"Uploading to s3://{bucket_name}")
    for file in filepath.iterdir():
        filename = str(file).split("/")[-1]

        # Upload to s3
        client.upload_file(Filename=file.as_posix(), Bucket=bucket_name, Key=f"{(object_key / filename).as_posix()}")

        logger.info(f"Uploaded file to: s3://{bucket_name}/{(object_key / filename).as_posix()}")

    # clear dir
    clear_dir(dir=filepath)
    pass
    

# Configs
# - delta
# - batchsize
# - bucketname

tmp = Path("./tmp")
batch_size = 5000
delta = 29

bucket_name = "open-crime-etl"
s3_path = Path(f"raw/ingest={datetime.now().strftime('%Y-%m-%d')}")

with DAG(
    dag_id="crime_etl",
    start_date=datetime(2025, 7, 14),
    schedule="@weekly",
    description="ETL for crimeAPI",
    default_args=default_args,
    catchup=False
) as dag:
    # Fetch Metadata If exists, else create MetaData table
    # t1 = EmptyOperator(task_id="fetch_metadata")

    # Fetch data from API
    t2 = PythonOperator(
        task_id="fetch_api_data",
        python_callable=fetch_data_api,
        op_kwargs={
            "delta" : delta,
            "pagesize" : batch_size,
            "save_path" : tmp,
        }
    )

    # Upload to s3
    t3 = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
        op_kwargs={
            "bucket_name" : bucket_name,
            "filepath" : tmp, 
            "object_key" : s3_path 
        }
    )

    # # Check Table if exists, else skip
    # t4 = EmptyOperator(task_id="check_table")

    # # Load from s3 to Dbs
    # # - Snowflake
    # t5_1 = EmptyOperator(task_id="load_from_s3_to_snowflake") 
    # # - Postgres
    # t5_2 = EmptyOperator(task_id="load_from_s3_to_postgres") 
    
    # # Validate both DBs are synced
    # t6 = EmptyOperator(task_id="validate_sync")


    t2 >> t3
    # t1 >> t2 >> t3 >> t4 >> [t5_1, t5_2] >> t6