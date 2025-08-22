import re
import json
import gzip
import shutil
import logging
from pathlib import Path
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta

logger = logging.getLogger(__name__)

def str_to_date(str_date: str, format: str = '%Y-%m-%dT%H:%M:%S.%f') -> datetime:
    return datetime.strptime(str_date, format)

def date_to_str(date: datetime, format: str = '%Y-%m-%dT%H:%M:%S.%f') -> str:
    return date.strftime(format)[:-3]

def create_filter(param):
    # This code is prone to break if it depends on the param type, bad practise >:(
    if isinstance(param, date):
        # Normal Ingestion, generate date range
        today_date = datetime.now().date()
        dates_to_process = [(param + timedelta(days=i)).strftime("%Y-%m-%d") for i in range((today_date - param).days+1)]
        
        load_dates = "|".join(map(re.escape, dates_to_process))

    elif isinstance(param, str):
        # Sync Ingestion
        load_dates = "|".join(map(re.escape, [param]))

    else:
        raise ValueError(f"Unsupported filter param type: {type(param)}")

    # Generate the regex
    regex = re.compile(rf"^raw/year=\d{{4}}/month=\d{{2}}/load_date=({load_dates})/.*")
    return regex
    
def generate_date_range(start_date: datetime = None, end_date: datetime = None, delta: relativedelta = None) -> list:
    date_range = []

    start_date = start_date if start_date else datetime(2024,1,1)
    end_date = end_date if end_date else datetime.now()
    delta = delta if delta else relativedelta(months=1)

    while start_date < end_date:
        if start_date + delta > end_date:
            date_range.append(
                {
                    'start_date' : date_to_str(start_date),
                    'end_date' : date_to_str(end_date)
                }
            )
        else:
            date_range.append(
                {
                    'start_date' : date_to_str(start_date),
                    'end_date' : date_to_str(start_date + delta)
                }
            )
        start_date += delta
        
    return date_range

def save_to_path(save_to: str, date: str, pagenum: int, data: list) -> None:
    
    parsed_date = str_to_date(date)

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