from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="complex_crime_etl",
    default_args=default_args,
    start_date=datetime(2025, 7, 20),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    def decide_backfill(**context):
        # Imagine we fetch this from Airflow Variable or external input
        backfill_flag = False  # or True, depending on logic
        if backfill_flag:
            return "backfill_task"
        else:
            return "check_for_new_data"

    decide_path = BranchPythonOperator(
        task_id="decide_backfill",
        python_callable=decide_backfill,
    )

    backfill_task = PythonOperator(
        task_id="backfill_task",
        python_callable=lambda: print("Performing heavy backfill..."),
    )

    def check_new_data(**context):
        # Here you’d check your API or data source
        new_data_available = True  # Or False
        # Push result to XCom
        context['ti'].xcom_push(key='new_data_available', value=new_data_available)
        return "ingest_data" if new_data_available else "skip_ingestion"

    check_data = PythonOperator(
        task_id="check_for_new_data",
        python_callable=check_new_data,
        provide_context=True,
    )

    ingest_data = PythonOperator(
        task_id="ingest_data",
        python_callable=lambda: print("Ingesting new data..."),
    )

    skip_ingestion = EmptyOperator(
        task_id="skip_ingestion"
    )

    # After ingestion or backfill, run transformations
    def transformations(**context):
        # Access XCom data to maybe modify behavior
        new_data = context['ti'].xcom_pull(task_ids='check_for_new_data', key='new_data_available')
        if new_data:
            print("Running transformations on new data")
        else:
            print("Running transformations on backfilled data or skipped ingestion")
        # Do transformations here...

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transformations,
        provide_context=True,
    )

    # Branch for analytics depending on transform success
    def decide_analytics_path(**context):
        # Could check some status or result
        return "run_analytics"  # or "skip_analytics"

    analytics_branch = BranchPythonOperator(
        task_id="decide_analytics_path",
        python_callable=decide_analytics_path,
    )

    run_analytics = PythonOperator(
        task_id="run_analytics",
        python_callable=lambda: print("Running analytics job..."),
    )

    skip_analytics = EmptyOperator(task_id="skip_analytics")

    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED)

    # DAG layout

    decide_path >> [backfill_task, check_data]
    check_data >> [ingest_data, skip_ingestion]
    backfill_task >> transform_task
    ingest_data >> transform_task
    skip_ingestion >> transform_task

    transform_task >> analytics_branch
    analytics_branch >> [run_analytics, skip_analytics] >> end
