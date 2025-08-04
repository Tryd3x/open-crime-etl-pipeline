import logging
from pathlib import Path
from datetime import datetime, timezone
from crimeapi.utils.helper import clear_dir

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

    # clear dir
    clear_dir(dir=source_path)
