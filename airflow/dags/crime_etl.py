from dotenv import load_dotenv
load_dotenv()

from airflow import DAG
from airflow.models import XCom
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

from crimeapi.common.connect import create_aws_conn
from crimeapi.utils.helper import generate_date_range, save_to_path, str_to_date, clear_dir

from crimeapi.db.postgres.db_postgres import PostgresExecutor
from crimeapi.db.snowflake.db_snowflake import SnowflakeExecutor

from crimeapi.utils.custom_exceptions import APIPageFetchError

import logging
logger = logging.getLogger(__name__)

default_args = {
    "retries" : 3,
    "retry_delay" : timedelta(seconds=10),
}

def initialize_run(executors: dict, run_id: str, configs: dict, **context):
    """ TODO
    - Need to modify the code/query that initializes the logs in the metastore and have it not retrieve run_id
    """
    pos_executor: PostgresExecutor = executors['postgres']
    snow_executor: SnowflakeExecutor = executors['snowflake']

    # Initialize run in postgres
    pos_last_source_update, pos_last_load = pos_executor.init_log(run_id, configs)

    # Initialize run in snowflake
    snow_last_source_update, snow_last_load = snow_executor.init_log(run_id, configs)

    # Check if they are in sync
    """
    If the dates are out of sync, suggested solutions:

    - Perform Sync before ingesting new data
    - db lagging needs to catchup, so has to load from s3 (since it has current snapshot)
    - Once Synced, proceed to ingest current snapshot
    - Validate sync for both dbs in the end

    For temporary solution:
    - Get most recent dates for both source and load i.e max(load_date) and max(source_updated_on)
    - Implement sync task later
    """

    # Return synced last_source_update, last_load and run_id
    return pos_last_source_update, pos_last_load

def create_tables(executors: dict):
    """ 
    This task ensures all the necessary tables are in place before proceeding downstream. Avoids arbitrary checks and creation of table downstream
    """
    tables = ['logs','crime', 'date']

    pos_executor: PostgresExecutor = executors['postgres']
    snow_executor: SnowflakeExecutor = executors['snowflake']
    
    # Tables to check for: pipeline_logs, crime, date
    postgres_tables = pos_executor.get_tables()
    snowflake_tables = snow_executor.get_tables() 
    logger.info(f"postgres: {postgres_tables}")
    logger.info(f"snowflake: {snowflake_tables}")

    for t in tables:
        # Check in postgres
        if t not in postgres_tables:
            logger.info(f"Table '{t}' does not exist'")
            pos_executor.create_table(f'create_{t}.sql')

        # Check in snowflake
        if t not in snowflake_tables:
            logger.info(f"Table '{t}' does not exist'")
            snow_executor.create_table(f'create_{t}.sql')
    
    
def fetch_metadata(executors: dict, configs: dict, **context):
    """ 
    This task initializes the pipeline, its metadata and xcom variables
    """

    ti = context['ti']
    run_id = ti.run_id
    bucket_name = configs.get("bucket_name")
    ingest_batchsize = configs.get('ingest_batchsize')
    load_batchsize = configs.get("load_batchsize")

    # Initialize run
    last_source_update, last_load_date = initialize_run(executors, run_id, configs)

    # Set Xcom params
    ti.xcom_push(key='ingest_batchsize', value=ingest_batchsize)
    ti.xcom_push(key='load_batchsize', value=load_batchsize)
    ti.xcom_push(key='s3_bucket', value=bucket_name)
    ti.xcom_push(key='source_last_updated_on', value=last_source_update)
    ti.xcom_push(key='last_load_date', value=last_load_date)

def decide_load_type(executors: dict, **context):
    """
    This task decides whether to perform full load or incremental load
    """

    ti = context['ti']
    run_id = ti.run_id
    pos_executor: PostgresExecutor = executors['postgres']
    snow_executor: SnowflakeExecutor = executors['snowflake']

    source_last_update = ti.xcom_pull(task_ids='fetch_metadata', key='source_last_updated_on')

    # Update Log
    mode = 'INCREMENT' if source_last_update else 'FULL'
    pos_executor.update_log(run_id=run_id, mode=mode)
    snow_executor.update_log(run_id=run_id, mode=mode)

    return 'incremental_load' if source_last_update else 'full_load'

def full_load(executors: dict, save_path: str, **context):
    """
    Performs full load from API to S3.

    Handles:
    - generation of dates to query the API
    - Avoids OOM by utilizing generator to fetch data from API
    - On failure, resume from last successful page
    - On Airflow retry, waits till retries are exhausted before pushing update to log table and clearing the xcom cache

    Enhancement:
    - What if the data already exists
    """

    ti = context['ti']
    run_id = ti.run_id
    pos_executor: PostgresExecutor = executors['postgres']
    snow_executor: SnowflakeExecutor = executors['snowflake']

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
            logger.info("Clearing XCom state")
            XCom.clear(dag_id=ti.dag_id, run_id=ti.run_id, task_id="full_load")

            # Update log
            pos_executor.update_log(run_id=run_id, status="FAILED")
            snow_executor.update_log(run_id=run_id, status="FAILED")
        raise    
    
def incremental_load(executors: dict, save_path: str, **context):
    """
    Performs incremental load from API to S3.

    Handles:
    - generation of dates to query the API
    - Avoids OOM by utilizing generator to fetch data from API
    - On failure, resume from last successful page
    - On Airflow retry, waits till retries are exhausted before pushing update to log table and clearing the xcom cache
    """

    ti = context['ti']
    pos_executor: PostgresExecutor = executors['postgres']
    snow_executor: SnowflakeExecutor = executors['snowflake']
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
            logger.info("Clearing XCom state")
            XCom.clear(dag_id=ti.dag_id, run_id=ti.run_id, task_id=ti.task_id)

            # Update log
            pos_executor.update_log(run_id=ti.run_id, status="FAILED")
            snow_executor.update_log(run_id=ti.run_id, status="FAILED")
        raise    

def upload_to_s3(executors: dict, client, source_path, destination_path, **context):
    """TODO
    - Need to add `try except` here to handle retries and log updates
    """
    ti = context['ti']
    bucket_name = ti.xcom_pull(task_ids='fetch_metadata', key='s3_bucket')

    # Upload
    upload_files_to_s3(client, bucket_name, source_path, destination_path)

    # Clear tmp directory - DONT CLEAR
    clear_dir(source_path) 

def load_s3_to_postgres(executors: dict, client, source_path, destination_path, **context):
    """
    This task loads data from s3 to postgres
    
    TODO
    - Need to add `try except` here to handle retries and log updates - DONE
    - Need some kind of logic to prevent re-inserts to database possibly creating duplicates or let the transformation layer handle duplicates - Insert on conflict handles duplicates
    - Clear tmp directory once upload has completed or retries exhausted
    """
    
    ti = context['ti']
    pos_executor: PostgresExecutor = executors['postgres']

    bucket_name = ti.xcom_pull(task_ids='fetch_metadata', key='s3_bucket')
    batch_insert_size = ti.xcom_pull(task_ids='fetch_metadata', key='load_batchsize')
    last_load_date = ti.xcom_pull(task_ids="fetch_metadata", key="last_load_date")

    # checkpoint params
    last_checkpoint = ti.xcom_pull('load_from_s3_to_postgres', key='last_checkpoint') or {}

    # Bulk Download from s3, use xcom to track if download was successful from last checkpoint
    if not last_checkpoint.get("download_successful"):
        download_files_from_s3(client=client, bucket_name=bucket_name, source_path=source_path, destination_path=destination_path, last_load_date=last_load_date)

    # Stream - Uncompress -> Load -> Transform -> Batch Insert
    logger.info("Starting Uncompress")
    for file in Path(destination_path).rglob("*.gz"):
        # Unzip
        try:
            with gzip.open(file.as_posix(), 'rt') as f:

                # Load
                logger.info(f"Loading JSON: {file.as_posix()} ")
                data = json.load(f)
                
                # Transform
                df = transform(data)
                
                # On Failure:
                # - Perform retry on that file itself

                # Batch Insert
                logger.info(f"Performing Batch insert: {file.as_posix()}")
                pos_executor.load_crime_data(batchsize=batch_insert_size, df=df)

        except Exception as e:
            logger.exception(e)
            ti.xcom_push(key='last_checkpoint', value={'download_successful' : True})

            if ti.try_number == ti.max_tries:    
                logger.info("Max tries reached")

                # Clear tmp directory
                clear_dir(destination_path)

                # Clear persistent states from xcom
                logger.info("Clearing XCom state")
                XCom.clear(dag_id=ti.dag_id, run_id=ti.run_id, task_id=ti.task_id)

                # Update log
                pos_executor.update_log(run_id=ti.run_id, status="FAILED")

            raise

        # Checkpoint
        # - remove the file if load is successful
        logger.info(f"Successful: {file.as_posix()}")
        file.unlink()
    
    if Path(destination_path).exists() and Path(destination_path).is_dir():
        Path(destination_path).exists()

def load_s3_to_snowflake(executors: dict, client, source_path, destination_path, **context):
    """This task loads data from s3 to snowflake"""

    ti = context['ti']
    snow_executor: SnowflakeExecutor = executors['snowflake']

    bucket_name = ti.xcom_pull(task_ids='fetch_metadata', key='s3_bucket')
    batch_insert_size = ti.xcom_pull(task_ids='fetch_metadata', key='load_batchsize')
    last_load_date = ti.xcom_pull(task_ids="fetch_metadata", key="last_load_date")

    # checkpoint params
    last_checkpoint = ti.xcom_pull('load_from_s3_to_snowflake', key='last_checkpoint') or {}

    # Bulk Download from s3, use xcom to track if download was successful from last checkpoint
    if not last_checkpoint.get("download_successful"):
        download_files_from_s3(client=client, bucket_name=bucket_name, source_path=source_path, destination_path=destination_path, last_load_date=last_load_date)

    # Stream - Uncompress -> Load -> Transform -> Batch Insert
    logger.info("Starting Uncompress")
    for file in Path(destination_path).rglob("*.gz"):
        # Unzip
        try:
            with gzip.open(file.as_posix(), 'rt') as f:

                # Load
                logger.info(f"Loading JSON: {file.as_posix()} ")
                data = json.load(f)
                
                # Transform
                df = transform(data)
                
                # On Failure:
                # - Perform retry on that file itself

                # Batch Insert
                logger.info(f"Performing Batch insert: {file.as_posix()}")
                snow_executor.load_crime_data(batchsize=batch_insert_size, df=df)

        except Exception as e:
            logger.exception(e)
            ti.xcom_push(key='last_checkpoint', value={'download_successful' : True})

            if ti.try_number == ti.max_tries:    
                logger.info("Max tries reached")

                # Clear tmp directory
                clear_dir(destination_path)

                # Clear persistent states from xcom
                logger.info("Clearing XCom state")
                XCom.clear(dag_id=ti.dag_id, run_id=ti.run_id, task_id=ti.task_id)

                # Update log
                snow_executor.update_log(run_id=ti.run_id, status="FAILED")
            raise

        # Checkpoint
        # - remove the file if load is successful
        logger.info(f"Successful: {file.as_posix()}")
        file.unlink()
        
    if Path(destination_path).exists() and Path(destination_path).is_dir():
        Path(destination_path).exists()

def update_metdata(executors: dict, **context):
    ti = context['ti']
    pos_executor: PostgresExecutor = executors['postgres']
    snow_executor = executors['snowflake']

    last_update = pos_executor.get_last_source_update()

    dag_run = ti.get_dagrun()
    all_upstream_task_ids = ti.task.get_flat_relatives(upstream=True)
    failed_task = False
    for task in all_upstream_task_ids:
        task_instance = dag_run.get_task_instance(task.task_id)
        if task_instance and task_instance.state in {State.FAILED, State.UPSTREAM_FAILED}:
            failed_task = True
            break

    status = 'FAILED' if failed_task else 'SUCCESS'

    pos_executor.update_log(run_id=ti.run_id, status=status, source_updated_on=last_update)
    snow_executor.update_log(run_id=ti.run_id, status=status, source_updated_on=last_update)

db_params = {
    "host": 'host.docker.internal',
    "port": '5433',
    "username": 'admin',
    "password": 'admin',
    "db": 'crime_db',
}

snow_params = {
    "account" : 'QYKNAZY-GD18580',
    "database" : 'crime_db',
    "schema" : 'PUBLIC',
    "role" : 'ACCOUNTADMIN',
    "warehouse" : 'crime_wh',
    "user" : 'hyderreza',
    "password" : os.getenv("SNOWFLAKE_DB_PASSWORD")
}

aws_params = {
    "access_key" : os.getenv("AWS_ACCESS_KEY_ID"),
    "secret_access_key" : os.getenv("AWS_SECRET_ACCESS_KEY"),
    "region" : os.getenv("AWS_REGION")
}

# Define seperate DB executors for each, Postgres and Snowflake
tmp = "./tmp"
s3_destination = "raw/"
postgres_template_path = "./include/sql/postgres"
snowflake_template_path = "./include/sql/snowflake"

postgres_db = PostgresExecutor(**db_params, template_path=postgres_template_path)
snowflake_db = SnowflakeExecutor(**snow_params, template_path=snowflake_template_path)
client = create_aws_conn(resource='s3', **aws_params)

executors = dict(postgres=postgres_db, snowflake=snowflake_db)
config = {
    "bucket_name" : "crime-etl",
    "ingest_batchsize" : 5000,
    "load_batchsize": 1000,
}

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
        op_kwargs={
            "executors" : executors
        }
    )
    
    # Fetch Metadata
    check_metadata = PythonOperator(
        task_id="fetch_metadata",
        python_callable=fetch_metadata,
        op_kwargs={
            "executors" : executors,
            "configs" : config
        }
    )

    # Decide if it is a full load or incremental load
    decide_load_type = BranchPythonOperator(
        task_id = "decide_load_type",
        python_callable=decide_load_type,
        op_kwargs={
            "executors" : executors
        }
    )

    full_data_load = PythonOperator(
        task_id="full_load",
        python_callable=full_load,
        op_kwargs={
            "executors" : executors,
            "save_path" : tmp
        }
    )

    incremental_data_load = PythonOperator(
        task_id="incremental_load",
        python_callable=incremental_load,
        op_kwargs={
            "executors" : executors,
            "save_path" : tmp
        }
    )

    # Upload to s3
    upload_s3 = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
        op_kwargs={
            "executors" : executors,
            "client" : client,
            "source_path" : tmp, 
            "destination_path" : s3_destination 
        },
        trigger_rule = TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    # Load from s3 to DBs
    # - Snowflake
    load_snowflake = PythonOperator(
        task_id="load_from_s3_to_snowflake",
        python_callable=load_s3_to_snowflake,
        op_kwargs={
            "executors" : executors,
            "client" : client,
            "source_path" : s3_destination, 
            "destination_path" : f"{tmp}/snowflake",
            "batch_insert_size" : config.get('batch_insert_size')
        }
    ) 

    # - Postgres
    load_postgres = PythonOperator(
        task_id="load_from_s3_to_postgres",
        python_callable=load_s3_to_postgres,
        op_kwargs={
            "executors" : executors,
            "client" : client,
            "source_path" : s3_destination, 
            "destination_path" : f"{tmp}/postgres",
            "batch_insert_size" : config.get('batch_insert_size')
        }
    ) 

    update_metadata = PythonOperator(
        task_id="update_metadata",
        python_callable=update_metdata,
        trigger_rule = 'all_done',
        op_kwargs={
            "executors" : executors
        }
    )

    # Trigger DBT here, possibly a container im not sure
    
    # # Validate both DBs are synced
    # If they arent in sync, trigger a task to sync database from s3 and update metastore
    # validate = EmptyOperator(task_id="validate_sync")

    check_table >> check_metadata >> decide_load_type >> [full_data_load, incremental_data_load]  >> upload_s3 >> [load_postgres, load_snowflake] >> update_metadata
