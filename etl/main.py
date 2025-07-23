from dotenv import load_dotenv
load_dotenv(dotenv_path='../.env')

from utils.logger import Logger
logger = Logger(__name__)

import os
import gzip
import json
import yaml
import boto3
import shutil
import requests
import pandas as pd
from pathlib import Path
from pprint import pprint
from datetime import datetime, timedelta

def fetch_crime_data(delta: int, pagesize: int):
    """ TODO
    - Implement retries if failed to connect to api server
    """

    # For Backfill
    # min_update = f"SELECT min(updated_on)"
    # max_update = f"SELECT max(updated_on)"
    
    last_update = (datetime.now() - timedelta(days=delta)).isoformat()[:-3]

    # Implementing CDC, batch incremental loads only
    query = f"SELECT * WHERE updated_on >= '{last_update}'"

    url = "https://data.cityofchicago.org/api/v3/views/crimes/query.json"
    headers = {
        'X-App-Token' : os.getenv('APP_TOKEN')
    }

    # Need to add retry logic
    results = []
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

        res = requests.post(
            url,
            json=body,
            headers=headers
        )

        if res.status_code != 200:
            break

        if res.json() == []:
            break
        
        if pagenum >= 15:
            break
        
        yield(res.json())
        pagenum += 1

def compress_save_data(path: Path, delta: int, pagesize: int):
    logger.info(f"Fetching data from API")
    # Iterates over each batch yielded by fetch_crime_data()
    count = 0
    for i, data in enumerate(fetch_crime_data(delta=delta, pagesize=pagesize)):
        filename = f"part-{i+1:04}.json.gz"
        filepath = path / filename

        # Staging batch in local storage for upload
        with gzip.open(filepath,'wt') as f:
            json.dump(data, f)
        logger.info(f"Saved file to: {filepath.as_posix()}")
        count+=1
    
    return count

def upload_to_s3(client, bucket_name: str, filepath: Path, object_key: Path ):
    logger.info(f"Uploading to s3://{bucket_name}")
    for file in filepath.iterdir():
        filename = str(file).split("/")[-1]

        # Upload to s3
        client.upload_file(Filename=file.as_posix(), Bucket=bucket_name, Key=f"{(object_key / filename).as_posix()}")
        logger.info(f"Uploaded file to: s3://{bucket_name}/{(object_key / filename).as_posix()}")

def clear_dir(dir: Path):
    # Purge the temp folder
    if dir.exists():
        logger.info("Clearing tmp")
        for item in dir.iterdir():
            item.unlink()
            logger.info(f"Deleted file: {item}")
        shutil.rmtree(dir)


def run_pipeline(bucket_name: str, pagesize: int, delta: int):
    tmp = Path('./tmp')
    s3_path = Path(f"raw/ingest={datetime.now().strftime('%Y-%m-%d')}")
    tmp.mkdir(parents=True, exist_ok=True)

    client = boto3.client('s3')

    logger.info(f"Initiating pipeline: ingest={datetime.now().strftime('%Y-%m-%d')}")
    logger.info(f"tmp folder missing. Created 'tmp'")

    batch_count = compress_save_data(path = tmp, pagesize = pagesize, delta = delta)

    upload_to_s3(client=client, bucket_name=bucket_name, filepath=tmp, object_key=s3_path)

    # Update Metadata in Snowflake
    #  - Check if log table exists, else create
    #  - Insert metadata into log table
    #  - Attributes: ingested_at, records, total_batch, batch_size, status, s3_location

    clear_dir()

    logger.info("Pipeline terminated")


if __name__ == '__main__':

    config = dict()

    # Reading Config
    config_path = Path('./config.yaml')
    if config_path.exists():
        logger.info(f"Loading Configuration: {config_path.as_posix()}")
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
        logger.info(f"Loaded Configuration")
    
    # Loading Config
    BUCKET_NAME = config.get('bucket_name', '')
    BATCH_SIZE = config.get('config', {}).get('batchsize', 5000)
    DELTA = config.get('test',{}).get('delta', 7)


    # Core Logic here
    run_pipeline()
