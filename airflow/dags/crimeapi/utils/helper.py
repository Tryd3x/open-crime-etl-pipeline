import json
import gzip
import shutil
import logging
from pathlib import Path
from datetime import datetime
from dateutil.relativedelta import relativedelta

logger = logging.getLogger(__name__)

def generate_date_range(start_date: datetime = None, end_date: datetime = None, delta: relativedelta = None) -> list:
    date_range = []

    start_date = start_date if start_date else datetime(2024,1,1)
    end_date = end_date if end_date else datetime.now()
    delta = delta if delta else relativedelta(months=1)

    while start_date < end_date:
        if start_date + delta > end_date:
            date_range.append(
                {
                    'start_date' : start_date.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3],
                    'end_date' : end_date.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
                }
            )
            break
        else:
            date_range.append(
                {
                    'start_date' : start_date.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3],
                    'end_date' : (start_date + delta).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
                }
            )
            start_date += delta
        
    return date_range

def save_to_path(save_to: str, date: str, pagenum: int, data: dict) -> None:
    """ TODO
    - Remap folder structure
    - Example: tmp/year/month/part
    """
    parsed_date = datetime.strptime(date, '%Y-%m-%dT%H:%M:%S.%f')

    root = Path(save_to) # ./tmp
    year = str(parsed_date.year) # ./2024
    month = f"{parsed_date.month:02}" # ./01

    path = root / year / month
    filename = f"part-{pagenum:04}.json.gz"
    filepath =  path / filename

    if not path.exists():
        logger.info(f"Missing {path.as_posix()}. Creating one...")
        path.mkdir(parents=True, exist_ok=True)

    # Staging batch in local storage for upload
    with gzip.open(filepath,'wt') as f:
        json.dump(data, f)
    logger.info(f"Saved file to: {filepath.as_posix()}")

def clear_dir(dir: str) -> None:
    # Purge the temp folder
    logger.info(f"Clearing directory: {dir}")
    dir = Path(dir)
    if dir.exists():
        for item in dir.iterdir():
            if item.is_file():
                item.unlink()
                logger.info(f"Deleted file: {item.as_posix()}")
            elif item.is_dir():
                shutil.rmtree(item)
                logger.info(f"Deleted directory: {item.as_posix()}")