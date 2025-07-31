import os
import boto3
import logging
import requests
from datetime import datetime, timedelta

from crimeapi.utils.helper import save_to_path

logger = logging.getLogger(__name__)

def fetch_data_api(delta, pagesize: int, save_path: str) -> int:
    """ TODO
    - Two modes: full or incremental
    - Default Date: 2024-01-01
    - Methodology: query month by month i.e relativedelta(months=1)
    """

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
            "includeSynthetic": True
        }
        res = requests.post(url, json=body, headers=headers)

        if res.status_code != 200:
            raise Exception(f"API returned status {res.status_code} at page {pagenum}")
        
        if pagenum >= 50:
            raise Exception("Reached page limit 50, stopping to prevent infinite loop")
        
        if res.json() == []:
            return pagenum-1
        
        # Save data to local storage
        save_to_path(path=save_path, pagenum=pagenum, data=res.json())
        
        pagenum += 1

