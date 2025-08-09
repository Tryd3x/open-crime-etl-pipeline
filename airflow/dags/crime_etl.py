from dotenv import load_dotenv
load_dotenv()

from airflow import DAG
from airflow.utils.state import State
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import PythonOperator, BranchPythonOperator

import os
import gzip
import json
from pathlib import Path
from datetime import datetime, timedelta, time

from crimeapi.extract import fetch_data_api
from crimeapi.load import upload_files_to_s3, download_files_from_s3
from crimeapi.transform import transform
from crimeapi.common.connect import create_postgres_conn, create_aws_conn
from crimeapi.utils.helper import generate_date_range, save_to_path, str_to_date, clear_dir
from crimeapi.db.helper import MetaData, initialize_run_log, update_run_log, load_to_postgres, get_last_source_update
from crimeapi.db.tables import create_log_table, create_crime_table, create_date_table
from crimeapi.utils.custom_exceptions import APIPageFetchError

import logging
logger = logging.getLogger(__name__)

default_args = {
    "retries" : 3,
    "retry_delay" : timedelta(seconds=10),
}

def create_tables(engine, **context):
    """ 
    This task ensures all the necessary tables are in place before proceeding downstream. Avoids arbitrary checks and creation of table downstream
    """

    # Tables to check for:
    # pipeline_logs, crime, date
    meta = MetaData()
    meta.reflect(engine)

    if 'pipeline_logs' not in meta.tables.keys():
        logger.info("Table 'pipeline_logs' does not exist'")
        create_log_table(engine=engine)
    
    if 'crime' not in meta.tables.keys():
        logger.info("Table 'crime' does not exist'")
        create_crime_table(engine=engine)

    if 'date' not in meta.tables.keys():
        logger.info("Table 'date' does not exist'")
        create_date_table(engine=engine)

def fetch_metadata(engine, configs: dict, **context):
    """ 
    This task initializes the pipeline, its metadata and xcom variables
    """

    ti = context['ti']
    bucket_name = configs.get("bucket_name")
    ingest_batchsize = configs.get('ingest_batchsize')
    load_batchsize = configs.get("load_batchsize")

    # Connect to DB
    meta = MetaData()
    meta.reflect(engine)

    # Initialize Log
    run_id, last_source_update, last_load_date = initialize_run_log(engine=engine, config=configs)

    # Set Xcom params
    ti.xcom_push(key='ingest_batchsize', value=ingest_batchsize)
    ti.xcom_push(key='load_batchsize', value=load_batchsize)
    ti.xcom_push(key='s3_bucket', value=bucket_name)

    ti.xcom_push(key='pipeline_run_id', value=run_id)
    ti.xcom_push(key='source_last_updated_on', value=last_source_update)
    ti.xcom_push(key='last_load_date', value=last_load_date)

def decide_load_type(engine, **context):
    """
    This task decides whether to perform full load or incremental load
    """

    ti = context['ti']

    run_id = ti.xcom_pull(task_ids='fetch_metadata', key='pipeline_run_id')
    source_last_update = ti.xcom_pull(task_ids='fetch_metadata', key='source_last_updated_on')

    # Update Log
    mode = 'INCREMENT' if source_last_update else 'FULL'
    update_run_log(engine=engine, run_id=run_id, mode=mode)

    return 'incremental_load' if source_last_update else 'full_load'

def full_load(engine, save_path: str, **context):
    """
    Performs full load from API to S3.

    Handles:
    - generation of dates to query the API
    - Avoids OOM by utilizing generator to fetch data from API
    - On failure, resume from last successful page
    - On Airflow retry, waits till retries are exhausted before pushing update to log table and clearing the xcom cache
    """

    ti = context['ti']
    run_id = ti.xcom_pull(task_ids='fetch_metadata', key='pipeline_run_id')
    batchsize = ti.xcom_pull(task_ids='fetch_metadata', key='ingest_batchsize')

    # Check if there are any persistent state in xcom to resume from
    last_checkpoint = ti.xcom_pull(task_ids="full_load", key="last_checkpoint") or {}
    last_page = last_checkpoint.get('last_page', None)

    # Generate dates to query
    start_date = str_to_date(last_checkpoint.get('last_date')) if last_checkpoint.get('last_date') else datetime(2025,1,1)
    end_date = datetime.now()
    date_ranges = generate_date_range(start_date=start_date, end_date=end_date)

    try:
        for dr in date_ranges:
            start = dr.get('start_date')
            end = dr.get('end_date')
            
            for pagenum, data in fetch_data_api(start_date=start, end_date=end, pagesize=batchsize, resume_page=last_page):

                # Save to path
                save_to_path(save_to=save_path, date=start, pagenum=pagenum, data=data)

    except APIPageFetchError as e:
        logger.exception("APIPageFetchError occured")

        # Persist date and pagenum on failure
        ti.xcom_push(key="last_checkpoint", value={'last_page' : e.pagenum, 'last_date' : e.date})
        
        if ti.try_number == ti.max_tries:
            logger.info("Max retries reached.")

            # Clear temp directory
            clear_dir(save_path)

            # Clear persistent states from xcom
            logging.info("Clearing XCom state")
            ti.xcom_clear()

            # Update log
            update_run_log(engine=engine, run_id=run_id, status="FAILED")
        raise    
    
def incremental_load(engine, save_path: str, **context):
    """
    Performs incremental load from API to S3.

    Handles:
    - generation of dates to query the API
    - Avoids OOM by utilizing generator to fetch data from API
    - On failure, resume from last successful page
    - On Airflow retry, waits till retries are exhausted before pushing update to log table and clearing the xcom cache
    """

    ti = context['ti']
    run_id = ti.xcom_pull(task_ids='fetch_metadata', key='pipeline_run_id')
    batchsize = ti.xcom_pull(task_ids='fetch_metadata', key='ingest_batchsize')

    # Check if there are any persistent state in xcom we can resume from
    last_checkpoint = ti.xcom_pull(task_ids="incremental_load", key="last_checkpoint") or {}
    last_page = last_checkpoint.get('last_page', None)
    last_date = last_checkpoint.get('last_date', None)
    last_source_update = ti.xcom_pull(task_ids='fetch_metadata', key='source_last_updated_on')

    # Generate dates to query 
    start_date = last_date or datetime.combine(last_source_update, time.min)
    end_date = datetime.now()
    date_ranges = generate_date_range(start_date=start_date, end_date=end_date)

    try:
        for dr in date_ranges:
            start = dr.get('start_date')
            end = dr.get('end_date')

            for pagenum, data in fetch_data_api(start_date=start, end_date=end, pagesize=batchsize, resume_page=last_page):

                # Save to path
                save_to_path(save_to=save_path, date=start, pagenum=pagenum, data=data)

    except APIPageFetchError as e:
        logger.exception("APIPageFetchError occured")

        # Persist date and pagenum on failure
        ti.xcom_push(key="last_checkpoint", value={'last_page' : e.pagenum, 'last_date' : e.date})
        
        if ti.try_number == ti.max_tries:
            logger.info("Max retries reached.")

            # Clear temp directory
            clear_dir(save_path)

            # Clear persistent states from xcom
            logging.info("Clearing XCom state")
            ti.xcom_clear()

            # Update log
            update_run_log(engine=engine, run_id=run_id, status="FAILED")
        raise    

def upload_to_s3(engine, client, source_path, destination_path, **context):
    """TODO
    - Need to add `try except` here to handle retries and log updates
    """
    ti = context['ti']
    bucket_name = ti.xcom_pull(task_ids='fetch_metadata', key='s3_bucket')

    # Upload
    upload_files_to_s3(client, bucket_name, source_path, destination_path)

    # Clear tmp directory
    clear_dir(source_path)

def load_s3_to_postgres(engine, client, source_path, destination_path, **context):
    """
    This task loads data from s3 to database
    
    TODO
    - Need to add `try except` here to handle retries and log updates
    - Need some kind of logic to prevent re-inserts to database possibly creating duplicates or let the transformation layer handle duplicates
    - Clear tmp directory once upload has completed or retries exhausted
    """
    
    ti = context['ti']
    bucket_name = ti.xcom_pull(task_ids='fetch_metadata', key='s3_bucket')
    batch_insert_size = ti.xcom_pull(task_ids='fetch_metadata', key='load_batchsize')
    last_load_date = ti.xcom_pull(task_ids="fetch_metadata", key="last_load_date")

    meta = MetaData()
    meta.reflect(engine)

    crime_table = meta.tables['crime']

    # Bulk Download from s3
    download_files_from_s3(client=client, bucket_name=bucket_name, source_path=source_path, destination_path=destination_path, last_load_date=last_load_date)

    # Stream - Uncompress -> Load -> Transform -> Batch Insert
    logger.info("Starting Uncompress")
    for file in Path(destination_path).rglob("*.gz"):
        # Unzip
        with gzip.open(file.as_posix(), 'rt') as f:

            # Load
            logger.info(f"Loading JSON: {file.as_posix()} ")
            data = json.load(f)
            
            # Transform
            df = transform(data)

            # Batch Insert
            logger.info(f"Performing Batch insert: {file.as_posix()}")
            load_to_postgres(engine=engine, batchsize=batch_insert_size, table=crime_table, df=df)
    
    # Clean up dir
    clear_dir(destination_path)

def load_s3_to_snowflake(**context):
    pass

def update_metdata(engine, **context):
    ti = context['ti']
    run_id = ti.xcom_pull(task_ids='fetch_metadata', key="pipeline_run_id")

    meta = MetaData()
    meta.reflect(bind=engine)

    last_update = get_last_source_update(engine)

    dag_run = ti.get_dagrun()
    all_upstream_task_ids = ti.task.get_flat_relatives(upstream=True)
    failed_task = False
    for task in all_upstream_task_ids:
        task_instance = dag_run.get_task_instance(task.task_id)
        if task_instance and task_instance.state in {State.FAILED, State.UPSTREAM_FAILED}:
            failed_task = True
            break

    status = 'FAILED' if failed_task else 'SUCCESS'

    update_run_log(
        engine=engine, 
        run_id=run_id, 
        status=status, 
        source_updated_on=last_update
    )

# Configs
# - delta
# - batchsize
# - bucketname

db_params = {
    "host": 'host.docker.internal',
    "port": '5433',
    "username": 'admin',
    "password": 'admin',
    "db": 'mydb',
}

aws_params = {
    "access_key" : os.getenv("AWS_ACCESS_KEY_ID"),
    "secret_access_key" : os.getenv("AWS_SECRET_ACCESS_KEY"),
    "region" : os.getenv("AWS_REGION")
}

engine = create_postgres_conn(**db_params)
client = create_aws_conn(resource='s3', **aws_params)
config = {
    "bucket_name" : "open-crime-etl",
    "ingest_batchsize" : 5000,
    "load_batchsize": 1000,
}

tmp = "./tmp"
s3_destination = "raw/"

with DAG(
    dag_id="crime_etl",
    start_date=datetime(2024, 1, 1),
    schedule="@weekly",
    description="ETL for crimeAPI",
    default_args=default_args,
    catchup=False
) as dag:
    
    # Check tables
    check_table = PythonOperator(
        task_id = "check_table",
        python_callable=create_tables,
        op_args={
            "engine" : engine
        }
    )
    
    # Fetch Metadata
    check_metadata = PythonOperator(
        task_id="fetch_metadata",
        python_callable=fetch_metadata,
        op_kwargs={
            "engine" : engine,
            "configs" : config
        }
    )

    # Decide if it is a full load or incremental load
    decide_load_type = BranchPythonOperator(
        task_id = "decide_load_type",
        python_callable=decide_load_type,
        op_kwargs={
            "engine" : engine
        }
    )

    full_data_load = PythonOperator(
        task_id="full_load",
        python_callable=full_load,
        op_kwargs={
            "engine" : engine,
            "save_path" : tmp
        }
    )

    incremental_data_load = PythonOperator(
        task_id="incremental_load",
        python_callable=incremental_load,
        op_kwargs={
            "engine" : engine,
            "save_path" : tmp
        }
    )

    # Upload to s3
    upload_s3 = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
        op_kwargs={
            "engine" : engine,
            "client" : client,
            "source_path" : tmp, 
            "destination_path" : s3_destination 
        },
        trigger_rule = TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    # # Load from s3 to Dbs
    # # - Snowflake
    # load_snowflake = EmptyOperator(task_id="load_from_s3_to_snowflake") 

    # - Postgres
    load_postgres = PythonOperator(
        task_id="load_from_s3_to_postgres",
        python_callable=load_s3_to_postgres,
        op_kwargs={
            "engine" : engine,
            "client" : client,
            "source_path" : s3_destination, 
            "destination_path" : tmp,
            "batch_insert_size" : config.get('batch_insert_size')
        }
    ) 

    update_metadata = PythonOperator(
        task_id="update_metadata",
        python_callable=update_metdata,
        trigger_rule = 'all_done',
        op_kwargs={
            "engine" : engine
        }
    )

    # Trigger DBT here, possibly a container im not sure
    
    # # Validate both DBs are synced
    # validate = EmptyOperator(task_id="validate_sync")

    check_table >> check_metadata >> decide_load_type >> [full_data_load, incremental_data_load]  >> upload_s3 >> load_postgres >> update_metadata
