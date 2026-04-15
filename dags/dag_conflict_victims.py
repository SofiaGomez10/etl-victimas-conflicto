from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from scripts.ingest_source1 import ingest_source1
from scripts.ingest_source2 import ingest_source2
from scripts.ingest_source3 import ingest_source3
from scripts.transform_source1 import transform_source1
from scripts.transform_source2 import transform_source2
from scripts.transform_source3 import transform_source3
from scripts.merge_sources import run_merge

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

    # ── Ingesta ───────────────────────────────────────────────────────────────

    task_ingest_f1 = PythonOperator(
        task_id="ingest_source1_cali",
        python_callable=ingest_source1,
        op_kwargs={
            "sqlite_path": "/opt/airflow/data/processed/Project_ETL.db",
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

    # ── Transformación ────────────────────────────────────────────────────────

    task_transform_f1 = PythonOperator(
        task_id="transform_source1_cali",
        python_callable=transform_source1,
        op_kwargs={
            "input_path": "/opt/airflow/data/processed/source1.parquet",
            "output_path": "/opt/airflow/data/processed/source1_transformed.parquet",
        },
    )

    task_transform_f2 = PythonOperator(
        task_id="transform_source2_santander",
        python_callable=transform_source2,
        op_kwargs={
            "input_path": "/opt/airflow/data/processed/source2.parquet",
            "output_path": "/opt/airflow/data/processed/source2_transformed.parquet",
        },
    )

    task_transform_f3 = PythonOperator(
        task_id="transform_source3_api",
        python_callable=transform_source3,
        op_kwargs={
            "input_path": "/opt/airflow/data/processed/source3.parquet",
            "output_path": "/opt/airflow/data/processed/source3_transformed.parquet",
        },
    )

    # ── Merge ─────────────────────────────────────────────────────────────────

    task_merge = PythonOperator(
        task_id="merge_sources",
        python_callable=run_merge,
        op_kwargs={
            "source1_path": "/opt/airflow/data/processed/source1_transformed.parquet",
            "source2_path": "/opt/airflow/data/processed/source2_transformed.parquet",
            "source3_path": "/opt/airflow/data/processed/source3_transformed.parquet",
            "output_path": "/opt/airflow/data/processed/dataset_final.parquet",
        },
    )

    # ── Validación Great Expectations ─────────────────────────────────────────

    task_validate = PythonOperator(
        task_id="validate_great_expectations",
        python_callable=lambda: None,  # pending - se conecta luego de definir GX
    )

    # ── Carga ─────────────────────────────────────────────────────────────────

    task_load = PythonOperator(
        task_id="load_to_mysql",
        python_callable=lambda: None,  # pending
    )

    # ── Flujo ─────────────────────────────────────────────────────────────────

    start >> [task_ingest_f1, task_ingest_f2, task_ingest_f3]

    task_ingest_f1 >> task_transform_f1
    task_ingest_f2 >> task_transform_f2
    task_ingest_f3 >> task_transform_f3

    [task_transform_f1, task_transform_f2, task_transform_f3] >> task_merge
    task_merge >> task_validate >> task_load >> end