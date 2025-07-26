import os
import boto3
import logging
import requests
from pathlib import Path
from datetime import datetime, timedelta

from crimeapi.utils.helper import save_to_path, clear_dir

logger = logging.getLogger(__name__)

def fetch_data_api(delta: int, pagesize: int, save_path: str) -> int:
    # Fetch data
    last_update = (datetime.now() - timedelta(days=delta)).isoformat()[:-3]

    query = f"SELECT * WHERE updated_on >= '{last_update}'"

    url = "https://data.cityofchicago.org/api/v3/views/crimes/query.json"
    headers = {'X-App-Token' : os.getenv('APP_TOKEN')}

    pagenum = 1
    logger.info("Fetching data from API")
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

        # Need to handle this better, too vague and mixed up
        if res.status_code != 200:
            raise Exception(f"API returned status {res.status_code} at page {pagenum}")
        
        if pagenum >= 50:
            raise Exception("Reached page limit 50, stopping to prevent infinite loop")
        
        if res.json() == []:
            return pagenum-1
        
        # Save data to local storage
        save_to_path(path=save_path, pagenum=pagenum, data=res.json())
        
        pagenum += 1

def upload_files_to_s3(bucket_name: str, source_path: str, destination_path: str) -> str:

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

    object_key = Path(destination_path) / Path(f"ingest={datetime.now().strftime('%Y-%m-%d')}")

    logger.info(f"Uploading to s3://{bucket_name}")
    for file in Path(source_path).iterdir():
        filename = str(file).split("/")[-1]

        # Upload to s3
        client.upload_file(Filename=file.as_posix(), Bucket=bucket_name, Key=f"{(object_key / filename).as_posix()}")

        logger.info(f"Uploaded file to: s3://{bucket_name}/{(object_key / filename).as_posix()}")

    # clear dir
    clear_dir(dir=source_path)

    return f"s3://{bucket_name}/{(object_key).as_posix()}"