import json
import gzip
import shutil
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

def save_to_path(path: str, pagenum: int, data):
    # Iterates over each batch yielded by fetch_crime_data()
    path = Path(path)
    filename = f"part-{pagenum:04}.json.gz"
    filepath = path / filename

    if not path.exists():
        logger.info(f"Missing {path.as_posix()}. Creating one...")
        path.mkdir(parents=True, exist_ok=True)

    # Staging batch in local storage for upload
    with gzip.open(filepath,'wt') as f:
        json.dump(data, f)
    logger.info(f"Saved file to: {filepath.as_posix()}")

def clear_dir(dir: str):
    # Purge the temp folder
    logger.info(f"Clearing directory: {dir}")
    dir = Path(dir)
    if dir.exists():
        for item in dir.iterdir():
            item.unlink()
            logger.info(f"Deleted: {item.as_posix()}")
        shutil.rmtree(dir)