"""Apache Airflow DAG: LogiMesh Supply Chain & Logistics Lakehouse Orchestration.

Orchestrates raw ingestion, Great Expectations data quality contracts,
Apache Iceberg snapshot registration, dbt model transformations,
and Google BigQuery external table synchronization.
"""

from datetime import datetime, timedelta

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.operators.bash import BashOperator
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False


default_args = {
    "owner": "logistics-data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

if AIRFLOW_AVAILABLE:
    with DAG(
        dag_id="logimesh_supply_chain_mesh",
        default_args=default_args,
        description="End-to-end multi-tier logistics lakehouse pipeline",
        schedule_interval="@daily",
        start_date=datetime(2026, 1, 1),
        catchup=False,
        tags=["supply-chain", "iceberg", "duckdb", "dbt", "great-expectations"],
    ) as dag:

        # Task 1: Ingest raw orders and inventory snapshots
        t1_ingest_raw = BashOperator(
            task_id="extract_and_stage_raw_data",
            bash_command="python -m src.orchestration.pipeline_runner --task ingest_raw",
        )

        # Task 2: Great Expectations Quality Sensor
        t2_quality_gate = BashOperator(
            task_id="validate_ingestion_data_quality",
            bash_command="python -m src.orchestration.pipeline_runner --task quality_gate",
        )

        # Task 3: Sync Iceberg Lakehouse Snapshots
        t3_iceberg_sync = BashOperator(
            task_id="sync_iceberg_snapshots",
            bash_command="python -m src.orchestration.pipeline_runner --task iceberg_sync",
        )

        # Task 4: Execute dbt Staging Transformations (DuckDB)
        t4_dbt_staging = BashOperator(
            task_id="run_dbt_staging_transformations",
            bash_command="python -m src.orchestration.pipeline_runner --task dbt_staging",
        )

        # Task 5: Execute dbt Marts Transformations
        t5_dbt_marts = BashOperator(
            task_id="run_dbt_marts_transformations",
            bash_command="python -m src.orchestration.pipeline_runner --task dbt_marts",
        )

        # Task 6: Run dbt Schema and Analytical Tests
        t6_dbt_tests = BashOperator(
            task_id="test_dbt_data_models",
            bash_command="python -m src.orchestration.pipeline_runner --task dbt_tests",
        )

        # Task 7: Google BigQuery External Table Manifest Sync
        t7_bigquery_sync = BashOperator(
            task_id="sync_bigquery_manifest",
            bash_command="python -m src.orchestration.pipeline_runner --task bigquery_sync",
        )

        # Task 8: Evaluate Stockout Risk Alerts
        t8_stockout_alerts = BashOperator(
            task_id="evaluate_stockout_risk_alerts",
            bash_command="python -m src.orchestration.pipeline_runner --task evaluate_alerts",
        )

        # Define Task Dependencies
        t1_ingest_raw >> t2_quality_gate >> t3_iceberg_sync
        t3_iceberg_sync >> t4_dbt_staging >> t5_dbt_marts >> t6_dbt_tests
        t6_dbt_tests >> [t7_bigquery_sync, t8_stockout_alerts]
