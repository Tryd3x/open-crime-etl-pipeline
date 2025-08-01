import os
import logging
import requests

from crimeapi.utils.custom_exceptions import APIPageFetchError

logger = logging.getLogger(__name__)

def fetch_data_api(start_date: str, end_date: str, pagesize: int, resume_page: int = None):

    query = f"SELECT * WHERE updated_on BETWEEN '{start_date}' AND '{end_date}'"
    url = "https://data.cityofchicago.org/api/v3/views/crimes/query.json"
    headers = {'X-App-Token' : os.getenv('APP_TOKEN')}

    logger.info("Fetching data from API")
    pagenum = resume_page if resume_page else 1
    while True:
        body = {
            'query' : query,
            'page' : {
                'pageNumber' : pagenum,
                'pageSize' : pagesize
            },
            "includeSynthetic": True
        }

        try:
            res = requests.post(url, json=body, headers=headers)
        except requests.RequestException as e:
            raise APIPageFetchError(f"Request failed at page {pagenum} for date {start_date}: {e}", pagenum=pagenum, date=start_date) from e            

        if res.status_code != 200:
            raise APIPageFetchError(f"API returned status {res.status_code} at page {pagenum}", pagenum=pagenum, date=start_date)
        
        data = res.json()

        if not data:
            break

        yield pagenum, data
        
        pagenum += 1
