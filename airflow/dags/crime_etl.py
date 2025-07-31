from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.state import State
from airflow.utils.trigger_rule import TriggerRule


from dotenv import load_dotenv
load_dotenv()

import os
from datetime import datetime, timedelta

from crimeapi.extract import fetch_data_api
from crimeapi.load import upload_files_to_s3
from crimeapi.common.connect import create_postgres_conn, create_aws_conn
from crimeapi.utils.helper import generate_date_range
from crimeapi.db.helper import MetaData, initialize_run_log, update_run_log
from crimeapi.db.tables import create_log_table

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
    run_id, last_updated = initialize_run_log(engine=engine, config=configs)

    ti.xcom_push(key='pipeline_run_id', value=run_id)
    ti.xcom_push(key='source_last_updated_on', value=last_updated)

def decide_load_type(**context):
    ti = context['ti']

    source_last_update = ti.xcom_pull(task_ids='fetch_metadata', key='source_last_updated_on')

    return 'incremental_load' if source_last_update else 'full_load'

def full_load(engine, batchsize: int, save_path: str, **context):
    # Criteria to fire this off
    # - no source_last_updated

    """
    This function possibly handles the generation of dates utilized to query the API and calls fetch_data_api()
    """
    ti = context['ti']
    start_date = datetime(2024,1,1)
    end_date = datetime.now()

    date_ranges = generate_date_range(start_date=start_date, end_date=end_date)

    # Iterate over each date_range, call fetch_data_api()
    try:
        for dr in date_ranges:
            start = dr.get('start_date')
            end = dr.get('end_date')
            fetch_data_api(start_date=start, end_date=end, pagesize=batchsize, save_path=save_path)
    except Exception as e:
        logger.error(e)
        run_id = ti.xcom_pull(task_ids='fetch_metadata', key='pipeline_run_id')
        update_run_log(engine=engine, run_id=run_id, status="FAILED")
        raise

def incremental_load(engine, batchsize: int, save_path: str, **context):
    # Crieteria to fire this off
    # - source_last_updated

    """
    This function possibly handles the generation of dates utilized to query the API and calls fetch_data_api()
    """
    
    ti = context['ti']
    start_date = ti.xcom_pull(task_ids='fetch_metadata', key='source_last_updated_on')
    end_date = datetime.now()

    date_ranges = generate_date_range(start_date=start_date, end_date=end_date)

    # Iterate over each date_range, call fetch_data_api()
    try:
        for dr in date_ranges:
            start = dr.get('start_date')
            end = dr.get('end_date')

            # Need to decouple pagenum since it is now incrementally loaded and you dont want to save it as a seperate part-xxxx.json.gz. Might need to fetch metadata from s3 key about the count and use that count by appending and storing in that same folder
            fetch_data_api(start_date=start, end_date=end, pagesize=batchsize, save_path=save_path)
    except Exception as e:
        logger.error(e)
        run_id = ti.xcom_pull(task_ids='fetch_metadata', key='pipeline_run_id')
        update_run_log(engine=engine, run_id=run_id, status="FAILED")
        raise

def load_s3_to_postgres(**context):
    # Check if crime table exists, else create them
    # Load from s3 into postgres
    pass

def load_s3_to_postgres(**context):
    pass

def update_metdata(engine, **context):
    
    ti = context['ti']
    
    meta = MetaData()
    meta.reflect(bind=engine)

    run_id = ti.xcom_pull(task_ids='fetch_metadata', key="pipeline_run_id")
    last_update = None # Fetch this from DB after inserting

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

config = {
    "batchsize" : 1000,
}

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

bucket_name = "open-crime-etl"
tmp = "./tmp"
s3_destination = "raw/"

s3_client = create_aws_conn(resource='s3', **aws_params)
engine = create_postgres_conn(**db_params)

with DAG(
    dag_id="crime_etl",
    start_date=datetime(2024, 1, 1),
    schedule="@weekly",
    description="ETL for crimeAPI",
    default_args=default_args,
    catchup=False
) as dag:
    
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
        python_callable=decide_load_type
    )

    full_data_load = PythonOperator(
        task_id="full_load",
        python_callable=full_load,
        op_kwargs={
            "engine": engine,
            "batchsize" : config['batchsize'],
            "save_path" : tmp
        }
    )

    incremental_data_load = PythonOperator(
        task_id="incremental_load",
        python_callable=incremental_load,
        op_kwargs={
            "engine": engine,
            "batchsize" : config['batchsize'],
            "save_path" : tmp
        }
    )

    # Upload to s3
    upload_s3 = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_files_to_s3,
        op_kwargs={
            "client" : s3_client,
            "bucket_name" : bucket_name,
            "source_path" : tmp, 
            "destination_path" : s3_destination 
        },
        trigger_rule = TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    # # Load from s3 to Dbs
    # # - Snowflake
    # load_snowflake = EmptyOperator(task_id="load_from_s3_to_snowflake") 
    # # - Postgres
    # load_postgres = EmptyOperator(task_id="load_from_s3_to_postgres") 

    # update_metadata = PythonOperator(
    #     task_id="update_metadata",
    #     python_callable=update_metdata,
    #     op_kwargs={
    #         "engine" : engine,
    #     },
    #     trigger_rule = 'all_done'
    # )
    
    # # Validate both DBs are synced
    # validate = EmptyOperator(task_id="validate_sync")

    # # Dag with full vs increment load
    check_metadata >> decide_load_type >> [full_data_load, incremental_data_load]  >> upload_s3 
    # >> [load_snowflake, load_postgres] >> update_metadata >> validate

    # check_metadata >> fetch_data >> upload_s3 >> update_metadata 