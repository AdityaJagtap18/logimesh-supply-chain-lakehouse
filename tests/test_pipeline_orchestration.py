"""Tests for the Airflow pipeline orchestrator and task execution."""

import pytest
from src.orchestration.pipeline_runner import PipelineRunner


def test_pipeline_runner_full_execution(tmp_path):
    runner = PipelineRunner(base_dir=str(tmp_path))
    manifest = runner.run_full_pipeline()

    assert manifest["overall_status"] == "SUCCESS"
    assert manifest["tasks_executed"] == 8
    assert manifest["total_duration_ms"] > 0

    task_statuses = {t["task_id"]: t["status"] for t in manifest["task_runs"]}
    assert task_statuses["extract_and_stage_raw_data"] == "SUCCESS"
    assert task_statuses["validate_ingestion_data_quality"] == "SUCCESS"
    assert task_statuses["sync_iceberg_snapshots"] == "SUCCESS"
    assert task_statuses["run_dbt_staging_transformations"] == "SUCCESS"
    assert task_statuses["run_dbt_marts_transformations"] == "SUCCESS"
    assert task_statuses["test_dbt_data_models"] == "SUCCESS"
    assert task_statuses["sync_bigquery_manifest"] == "SUCCESS"
    assert task_statuses["evaluate_stockout_risk_alerts"] == "SUCCESS"
