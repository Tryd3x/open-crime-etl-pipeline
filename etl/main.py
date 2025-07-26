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
from time import time
from pathlib import Path
from datetime import datetime, timedelta

def benchmark(function):
    def wrapper(*args, **kwargs):
        start = time()
        function(*args, **kwargs)
        stop = time()
        logger.info(f"Finished in {stop-start: .2f} seconds")
    return wrapper

def fetch_crime_data(delta: int, pagesize: int):
    """ TODO
    - Implement retries if failed to connect to api server
    """

    last_update = (datetime.now() - timedelta(days=delta)).isoformat()[:-3]

    # Implementing CDC, batch incremental loads only
    query = f"SELECT * WHERE updated_on >= '{last_update}'"

    url = "https://data.cityofchicago.org/api/v3/views/crimes/query.json"
    headers = {'X-App-Token' : os.getenv('APP_TOKEN')}

    # Need to add retry logic
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

        if res.status_code != 200 or res.json() == [] or pagenum >= 50:
            break
        
        yield(res.json())
        pagenum += 1

def save_to_path(path: Path, delta: int, pagesize: int):
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

def upload_to_s3(bucket_name: str, filepath: Path, object_key: Path ):
    logger.info(f"Uploading to s3://{bucket_name}")
    client = boto3.client('s3')
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

@benchmark
def run_pipeline(configs: dict):
    tmp = Path('./tmp')
    s3_path = Path(f"raw/ingest={datetime.now().strftime('%Y-%m-%d')}")
    if not tmp.exists():
        logger.info(f"tmp folder missing. Created 'tmp'")
        tmp.mkdir(parents=True, exist_ok=True)

    # Load config
    BUCKET_NAME = configs.get('aws', {}).get('bucket_name', '')
    
    config = configs.get("config",{})
    BATCH_SIZE = config.get('batchsize', 5000)
    DELTA = config.get('delta', 7)

    logger.info(f"Initiating pipeline: ingest={datetime.now().strftime('%Y-%m-%d')}")
    # Update Metadata in Snowflake
    #  - Check if log table exists, else create
    #  - Insert metadata into log table
    #  - Attributes: ingested_at, records, total_batch, batch_size, status, s3_location


    try:

        batch_count = save_to_path(path = tmp, pagesize = BATCH_SIZE, delta = DELTA)

        upload_to_s3(bucket_name=BUCKET_NAME, filepath=tmp, object_key=s3_path)

        clear_dir(dir=tmp)

        logger.info("Pipeline terminated")

    except Exception as e:
        logger.warn(f"Exception thrown: {e}")


if __name__ == '__main__':

    configs = dict()

    # Reading Config
    config_path = Path('./config.yml')
    if config_path.exists():
        logger.info(f"Loading Configuration: {config_path.as_posix()}")
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
        logger.info(f"Loaded Configuration")
    
    # Core Logic here
    run_pipeline(configs=configs)

    exit(0)
