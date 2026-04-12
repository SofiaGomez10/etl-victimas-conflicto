from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from scripts.ingest_source1 import ingest_source1
from scripts.ingest_source2 import ingest_source2
from scripts.ingest_source3 import ingest_source3

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dag_armed_conflict_victims",
    default_args=default_args,
    description="ETL pipeline for armed conflict victims data in Colombia",
    schedule_interval="0 6 * * *",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "victims", "sdg16"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    task_ingest_f1 = PythonOperator(
        task_id="ingest_source1_cali",
        python_callable=ingest_source1,
        op_kwargs={
            "sqlite_path": "/opt/airflow/data/processed/Project_ETL.db",
            "csv_path": "/opt/airflow/data/raw/data-population-victims-of-armed-conflict.csv",
            "output_path": "/opt/airflow/data/processed/source1.parquet",
        },
    )

    task_ingest_f2 = PythonOperator(
        task_id="ingest_source2_santander",
        python_callable=ingest_source2,
        op_kwargs={
            "csv_path": "/opt/airflow/data/raw/santander_victims.csv",
            "output_path": "/opt/airflow/data/processed/source2.parquet",
        },
    )

    task_ingest_f3 = PythonOperator(
        task_id="ingest_source3_api",
        python_callable=ingest_source3,
        op_kwargs={
            "url": "https://www.datos.gov.co/resource/ynab-fjc9.json",
            "start_year": 2012,
            "output_path": "/opt/airflow/data/processed/source3.parquet",
        },
    )

    task_transform = PythonOperator(
        task_id="transform_and_merge",
        python_callable=lambda: None,  # pending
    )

    task_validate = PythonOperator(
        task_id="validate_great_expectations",
        python_callable=lambda: None,  # pending
    )

    task_load = PythonOperator(
        task_id="load_to_mysql",
        python_callable=lambda: None,  # pending
    )

    start >> [task_ingest_f1, task_ingest_f2, task_ingest_f3]
    [task_ingest_f1, task_ingest_f2, task_ingest_f3] >> task_transform
    task_transform >> task_validate >> task_load >> end