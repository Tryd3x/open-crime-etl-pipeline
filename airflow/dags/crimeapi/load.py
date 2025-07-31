import logging
from pathlib import Path
from datetime import datetime
from crimeapi.utils.helper import clear_dir

logger = logging.getLogger(__name__)

def upload_files_to_s3(client, bucket_name: str, source_path: str, destination_path: str) -> str:

    # Modify object key to store by year/month
    # Example Key: raw/year=2025/month=01/data.json.gz

    # new key
    object_key = Path(destination_path) / Path(f"year=/") / Path('month=/')
    
    # old key
    # object_key = Path(destination_path) / Path(f"ingest={datetime.now().strftime('%Y-%m-%d')}")

    bucket = client.Bucket(bucket_name)

    logger.info(f"Uploading to s3://{bucket_name}")
    for file in Path(source_path).iterdir():
        filename = str(file).split("/")[-1]

        # Upload to s3
        bucket.upload_file(file.as_posix(), f"{(object_key / filename).as_posix()}")

        logger.info(f"Uploaded file to: s3://{bucket_name}/{(object_key / filename).as_posix()}")

    # clear dir
    clear_dir(dir=source_path)

    return f"s3://{bucket_name}/{(object_key).as_posix()}"