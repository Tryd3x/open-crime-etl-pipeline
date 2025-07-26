from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.state import State

from dotenv import load_dotenv
load_dotenv()

from pathlib import Path
from datetime import datetime, timedelta

from crimeapi.extract import fetch_data_api, upload_files_to_s3
from crimeapi.db.helper import MetaData, create_postgres_conn, create_log_table, initialize_run_log, update_run_log

import logging
logger = logging.getLogger(__name__)

default_args = {
    "retries" : 3,
    "retry_delay" : timedelta(seconds=10),
}

def fetch_metadata(engine, configs: dict, **context):
    ti = context['ti']

    # Connect to DB
    meta = MetaData()
    meta.reflect(engine)

    # Check if table exists, else create one
    if 'pipeline_logs' not in meta.tables.keys():
        logger.info("Table 'pipeline_logs' does not exist'")
        create_log_table(engine) 
    
    # Initialize Log
    run_id = initialize_run_log(engine=engine, config=configs)

    ti.xcom_push(key='pipeline_run_id', value=run_id) 

def fetch_data_from_api(engine, delta: int, pagesize: int, save_path: str, **context):
    ti = context['ti']
    try:
        pagenum = fetch_data_api(delta, pagesize, save_path)
        ti.xcom_push(key="batch_count", value=pagenum)
    except Exception as e:
        logger.error(e)
        run_id = ti.xcom_pull(task_ids='fetch_metadata', key='pipeline_run_id')
        update_run_log(engine=engine, run_id=run_id, status="FAILED")
        raise

def upload_to_s3(bucket_name, source_path, destination_path, **context):
    ti = context['ti']

    file_location = upload_files_to_s3(bucket_name, source_path, destination_path)

    ti.xcom_push(key='file_location', value=file_location)

def update_metdata(engine, **context):
    
    ti = context['ti']
    
    meta = MetaData()
    meta.reflect(bind=engine)

    run_id = ti.xcom_pull(task_ids='fetch_metadata', key="pipeline_run_id")
    batch_count = ti.xcom_pull(task_ids='fetch_api_data', key="batch_count")
    file_location = ti.xcom_pull(task_ids='upload_to_s3', key="file_location")

    dag_run = ti.get_dagrun()
    upstream_task_ids = ti.task.upstream_task_ids
    failed_task = False
    for task_id in upstream_task_ids:
        task_instance = dag_run.get_task_instance(task_id)
        if task_instance and task_instance.state in {State.FAILED, State.UPSTREAM_FAILED}:
            failed_task = True

    status = 'FAILED' if failed_task else 'SUCCESS'

    update_run_log(engine=engine, run_id=run_id, status=status, batch_count=batch_count, file_location=file_location)

# Configs
# - delta
# - batchsize
# - bucketname

batch_size = 1000
delta = 29
db_params = {
    "host": 'host.docker.internal',
    "port": '5433',
    "username": 'admin',
    "password": 'admin',
    "db": 'mydb',
}

bucket_name = "open-crime-etl"
tmp = "./tmp"
s3_destination = "raw/"

engine = create_postgres_conn(**db_params)

with DAG(
    dag_id="crime_etl",
    start_date=datetime(2025, 7, 14),
    schedule="@weekly",
    description="ETL for crimeAPI",
    default_args=default_args,
    catchup=False
) as dag:
    
    # Fetch Metadata
    t1 = PythonOperator(
        task_id="fetch_metadata",
        python_callable=fetch_metadata,
        op_kwargs={
            "engine" : engine,
            "configs" : {
                "batchsize" : batch_size,
                "delta" : delta
            }
        }
    )

    # Fetch data from API
    t2 = PythonOperator(
        task_id="fetch_api_data",
        python_callable=fetch_data_from_api,
        op_kwargs={
            "engine" : engine,
            "delta" : delta,
            "pagesize" : batch_size,
            "save_path" : tmp,
        }
    )

    # Upload to s3
    t3 = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
        op_kwargs={
            "bucket_name" : bucket_name,
            "source_path" : tmp, 
            "destination_path" : s3_destination 
        }
    )

    # # Check Table if exists, else skip
    # t4 = EmptyOperator(task_id="check_table")

    # # Load from s3 to Dbs
    # # - Snowflake
    # t5_1 = EmptyOperator(task_id="load_from_s3_to_snowflake") 
    # # - Postgres
    # t5_2 = EmptyOperator(task_id="load_from_s3_to_postgres") 

    t6 = PythonOperator(
        task_id="update_metadata",
        python_callable=update_metdata,
        op_kwargs={
            "engine" : engine,
        },
        trigger_rule = 'all_done'
    )
    
    # Validate both DBs are synced
    # t7 = EmptyOperator(task_id="validate_sync")


    t1 >> t2 >> t3 >> t6
    # t1 >> t2 >> t3 >> t4 >> [t5_1, t5_2] >> t6