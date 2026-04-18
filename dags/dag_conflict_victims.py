# Import Airflow classes and custom ETL scripts for pipeline orchestration
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scripts.ingest_source1 import ingest_source1
from scripts.ingest_source2 import ingest_source2
from scripts.ingest_source3 import ingest_source3
from scripts.transform_source1 import transform_source1
from scripts.transform_source2 import transform_source2
from scripts.transform_source3 import transform_source3
from scripts.concat_sources import run_concat
from scripts.consolidate import consolidate
from scripts.validate import validate_all
from scripts.load import load_to_mysql

# Default arguments applied to all tasks in the DAG
default_args = {
    "retries": 2, # Retry up to 2 times on failure
    "retry_delay": timedelta(minutes=2), # Wait 2 minutes between retries
}

with DAG(
    dag_id="dag_armed_conflict_victims",
    default_args=default_args,
    description="ETL pipeline for armed conflict victims data in Colombia",
    schedule_interval="0 6 * * *",  # Runs daily at 06:00 
    start_date=datetime(2026, 4, 1),
    catchup=False,
    max_active_runs=1,   # Only one DAG run active at a time
    tags=["etl", "victims", "sdg16"],
) as dag:

    # ── Ingestion ───────────────────────────────────────────────────────────────

    # Ingest source 1: local SQLite database (Cali)
    task_ingest_f1 = PythonOperator(
        task_id="ingest_source1_cali",
        python_callable=ingest_source1,
        op_kwargs={
            "sqlite_path": "/opt/airflow/data/processed/Project_ETL.db",
            "output_path": "/opt/airflow/data/processed/source1.parquet",
        },
    )

    # Ingest source 2: CSV file (Santander)
    task_ingest_f2 = PythonOperator(
        task_id="ingest_source2_santander",
        python_callable=ingest_source2,
        op_kwargs={
            "csv_path": "/opt/airflow/data/raw/santander_victims.csv",
            "output_path": "/opt/airflow/data/processed/source2.parquet",
        },
    )

    # Ingest source 3: REST API from datos.gov.co (Colombia)
    task_ingest_f3 = PythonOperator(
        task_id="ingest_source3_api",
        python_callable=ingest_source3,
        op_kwargs={
            "url": "https://www.datos.gov.co/resource/ynab-fjc9.json",
            "start_year": 2012,
            "output_path": "/opt/airflow/data/processed/source3.parquet",
        },
    )

    # ── Transformation ────────────────────────────────────────────────────────
   
    # Normalize and standardize source 1 (Cali)
    task_transform_f1 = PythonOperator(
        task_id="transform_source1_cali",
        python_callable=transform_source1,
        op_kwargs={
            "input_path": "/opt/airflow/data/processed/source1.parquet",
            "output_path": "/opt/airflow/data/processed/source1_transformed.parquet",
        },
    )

    # Normalize and standardize source 2 (Santander)
    task_transform_f2 = PythonOperator(
        task_id="transform_source2_santander",
        python_callable=transform_source2,
        op_kwargs={
            "input_path": "/opt/airflow/data/processed/source2.parquet",
            "output_path": "/opt/airflow/data/processed/source2_transformed.parquet",
        },
    )

    # Normalize and standardize source 3 (Colombia API)
    task_transform_f3 = PythonOperator(
        task_id="transform_source3_api",
        python_callable=transform_source3,
        op_kwargs={
            "input_path": "/opt/airflow/data/processed/source3.parquet",
            "output_path": "/opt/airflow/data/processed/source3_transformed.parquet",
        },
    )

    # ── Concatenation ─────────────────────────────────────────────────────────

    # Merge all three transformed sources into a single dataset
    task_concat = PythonOperator(
        task_id="concat_sources",
        python_callable=run_concat,
        op_kwargs={
            "source1_path": "/opt/airflow/data/processed/source1_transformed.parquet",
            "source2_path": "/opt/airflow/data/processed/source2_transformed.parquet",
            "source3_path": "/opt/airflow/data/processed/source3_transformed.parquet",
            "output_path": "/opt/airflow/data/processed/dataset_final.parquet",
        },
    )

    # ── Consolidation ─────────────────────────────────────────────────────────

    # Group and aggregate victim counts to remove duplicates
    task_consolidate = PythonOperator(
        task_id="consolidate_dataset",
        python_callable=consolidate,
        op_kwargs={
            "input_path": "/opt/airflow/data/processed/dataset_final.parquet",
            "output_path": "/opt/airflow/data/processed/dataset_consolidated.parquet",
        },
    )

    # ── Validation Great Expectations ─────────────────────────────────────────

    # Run data quality checks against the consolidated dataset
    task_validate = PythonOperator(
        task_id="validate_great_expectations",
        python_callable=validate_all,
        op_kwargs={
            "dataset_final_path": "/opt/airflow/data/processed/dataset_consolidated.parquet",
            "gx_root": "/opt/airflow/great_expectations",
        },
    )

    # ── Load ─────────────────────────────────────────────────────────────────

    # Load the validated dataset into MySQL
    task_load = PythonOperator(
        task_id="load_to_mysql",
        python_callable=load_to_mysql,
        op_kwargs={
            "dataset_path": "/opt/airflow/data/processed/dataset_consolidated.parquet",
        },
    )

    # ── DAG flow ─────────────────────────────────────────────────────────────────
    # Each source is ingested and transformed independently in parallel
    task_ingest_f1 >> task_transform_f1
    task_ingest_f2 >> task_transform_f2
    task_ingest_f3 >> task_transform_f3

    # Once all sources are transformed, concatenate → consolidate → validate → load
    [task_transform_f1, task_transform_f2, task_transform_f3] >> task_concat
    task_concat >> task_consolidate >> task_validate >> task_load