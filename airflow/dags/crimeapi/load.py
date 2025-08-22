import gzip
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from crimeapi.transform import transform

logger = logging.getLogger(__name__)

def upload_files_to_s3(client, bucket_name: str, source_path: str, destination_path: str) -> None:

    bucket = client.Bucket(bucket_name)
    load_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    logger.info(f"Uploading to s3://{bucket_name}")
    for path in Path(source_path).rglob("*.gz"):
        year, month, file = path.as_posix().split("/")[1:]
        key = destination_path / Path(f"year={year}/") / Path(f"month={month}/") / Path(f"load_date={load_date}/") / Path(file)

        # Upload to s3
        bucket.upload_file(path.as_posix(), key.as_posix())

        logger.info(f"Uploaded file to: s3://{bucket_name}/{key.as_posix()}")

def download_files_from_s3(client, bucket_name: str, source_path: str, destination_path: str, filter: str):

    bucket = client.Bucket(bucket_name)
    destination_path = Path(destination_path)

    logger.info(f"Downloading files from s3://{bucket_name}/{source_path} to {destination_path}")
    for i in bucket.objects.filter(Prefix=source_path):
        if filter.match(i.key):
            key = i.key.split("/")
            year = key[1]
            month = key[2]
            ingested_at = key[3]
            file = key[-1]

            # Create tmp directory
            if not destination_path.exists():
                logger.info("Missing directory 'tmp'. Creating one")
                destination_path.mkdir(parents=True, exist_ok=True)

            filename = f"{ingested_at.split('=')[-1]}_{year[-4:]}{month[-2:]}_{file}"

            logger.info(f"Downloaded file: {filename}")
            bucket.download_file(i.key, (destination_path / filename))

def load_crime(pos_executor, batch_insert_size, file):
    with gzip.open(file.as_posix(), 'rt') as f:
        # Load
        logger.info(f"Loading JSON: {file.as_posix()} ")
        data = json.load(f)
                
        # Transform
        df = transform(data)
                
        # Batch Insert
        logger.info(f"Performing Batch insert: {file.as_posix()}")
        pos_executor.load_crime_data(batchsize=batch_insert_size, df=df)