"""Deterministic pipeline orchestrator and task runner for LogiMesh.

Executes the Airflow task graph sequentially, tracking task states,
execution duration, and outputs for both CLI and Streamlit UI.
"""

import argparse
from datetime import datetime, timezone
import json
import os
import sys
import time
from typing import Any, Dict, List, Optional

from src.core.generator import SupplyChainDataGenerator
from src.core.iceberg_manager import IcebergTableManager
from src.core.quality_gate import DataQualityGate
from src.core.duckdb_engine import DuckDBAnalyticsEngine
from src.warehouse.bigquery_manager import BigQueryWarehouseManager


class PipelineRunner:
    """Orchestrates end-to-end Lakehouse pipeline tasks mirroring Airflow DAG execution."""

    TASKS = [
        ("extract_and_stage_raw_data", "Generate and stage multi-tier orders, inventory & warehouse datasets"),
        ("validate_ingestion_data_quality", "Enforce Great Expectations data quality contracts and schemas"),
        ("sync_iceberg_snapshots", "Register ACID Lakehouse snapshots and partition metadata in Apache Iceberg"),
        ("run_dbt_staging_transformations", "Execute dbt staging SQL models in DuckDB OLAP engine"),
        ("run_dbt_marts_transformations", "Compute dimensional models and stockout risk marts in DuckDB"),
        ("test_dbt_data_models", "Verify dbt data integrity tests (uniqueness, not null, status domains)"),
        ("sync_bigquery_manifest", "Generate Google BigQuery external table DDLs and cluster specs"),
        ("evaluate_stockout_risk_alerts", "Evaluate critical stockout alerts and capital risk metrics"),
    ]

    def __init__(self, base_dir: Optional[str] = None):
        self.base_dir = base_dir or os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        self.raw_dir = os.path.join(self.base_dir, "data", "raw")
        self.iceberg_dir = os.path.join(self.base_dir, "data", "iceberg")
        self.duckdb_path = os.path.join(self.base_dir, "data", "duckdb", "logimesh.duckdb")
        self.outputs_dir = os.path.join(self.base_dir, "outputs")
        os.makedirs(self.outputs_dir, exist_ok=True)

    def task_ingest_raw(self) -> Dict[str, Any]:
        """Task 1: Ingest raw data."""
        t0 = time.perf_counter()
        gen = SupplyChainDataGenerator(seed=42)
        dfs = gen.generate_full_dataset(raw_output_dir=self.raw_dir)
        elapsed = (time.perf_counter() - t0) * 1000.0

        return {
            "status": "SUCCESS",
            "duration_ms": round(elapsed, 2),
            "tables_ingested": {k: len(v) for k, v in dfs.items()},
            "raw_dir": self.raw_dir,
        }

    def task_quality_gate(self) -> Dict[str, Any]:
        """Task 2: Great Expectations assertions."""
        t0 = time.perf_counter()
        gate = DataQualityGate(reports_dir=os.path.join(self.outputs_dir, "quality_reports"))
        gen = SupplyChainDataGenerator(seed=42)
        wh = gen.generate_warehouses_df()
        orders, items = gen.generate_orders_and_items()
        inv = gen.generate_inventory_snapshots(orders, items)

        report = gate.run_full_suite(orders, items, inv)
        elapsed = (time.perf_counter() - t0) * 1000.0

        return {
            "status": "SUCCESS" if report["overall_status"] == "PASSED" else "FAILED",
            "duration_ms": round(elapsed, 2),
            "total_assertions": report["total_assertions"],
            "passed_assertions": report["passed_assertions"],
            "pass_rate_pct": report["pass_rate_pct"],
        }

    def task_iceberg_sync(self) -> Dict[str, Any]:
        """Task 3: Iceberg snapshots."""
        t0 = time.perf_counter()
        mgr = IcebergTableManager(warehouse_path=self.iceberg_dir)
        gen = SupplyChainDataGenerator(seed=42)
        orders, items = gen.generate_orders_and_items(order_count=200)
        wh = gen.generate_warehouses_df()

        r1 = mgr.create_or_append_table("orders", orders, partition_col="order_status")
        r2 = mgr.create_or_append_table("order_items", items, partition_col="warehouse_id")
        r3 = mgr.create_or_append_table("warehouses", wh)
        elapsed = (time.perf_counter() - t0) * 1000.0

        return {
            "status": "SUCCESS",
            "duration_ms": round(elapsed, 2),
            "tables_synced": ["orders", "order_items", "warehouses"],
            "orders_snapshot_id": r1["snapshot_id"],
            "items_snapshot_id": r2["snapshot_id"],
        }

    def task_dbt_staging(self) -> Dict[str, Any]:
        """Task 4: dbt staging views in DuckDB."""
        t0 = time.perf_counter()
        engine = DuckDBAnalyticsEngine(db_path=self.duckdb_path)
        engine.register_parquet_sources(raw_data_dir=self.raw_dir)
        engine.execute_staging_transformations()
        df, _ = engine.query("SELECT COUNT(*) AS cnt FROM stg_orders;")
        stg_orders_count = int(df["cnt"].iloc[0])
        engine.close()
        elapsed = (time.perf_counter() - t0) * 1000.0

        return {
            "status": "SUCCESS",
            "duration_ms": round(elapsed, 2),
            "views_created": ["stg_orders", "stg_order_items", "stg_inventory"],
            "stg_orders_count": stg_orders_count,
        }

    def task_dbt_marts(self) -> Dict[str, Any]:
        """Task 5: dbt analytical marts in DuckDB."""
        t0 = time.perf_counter()
        engine = DuckDBAnalyticsEngine(db_path=self.duckdb_path)
        engine.register_parquet_sources(raw_data_dir=self.raw_dir)
        engine.execute_staging_transformations()
        engine.execute_marts_transformations()
        df_risk, _ = engine.query("SELECT COUNT(*) AS cnt FROM fct_stockout_risks;")
        risk_count = int(df_risk["cnt"].iloc[0])
        engine.close()
        elapsed = (time.perf_counter() - t0) * 1000.0

        return {
            "status": "SUCCESS",
            "duration_ms": round(elapsed, 2),
            "tables_created": ["dim_warehouses", "fct_delivery_performance", "fct_stockout_risks"],
            "fct_stockout_risks_count": risk_count,
        }

    def task_dbt_tests(self) -> Dict[str, Any]:
        """Task 6: dbt schema and data integrity tests."""
        t0 = time.perf_counter()
        engine = DuckDBAnalyticsEngine(db_path=self.duckdb_path)
        engine.register_parquet_sources(raw_data_dir=self.raw_dir)
        engine.execute_staging_transformations()
        engine.execute_marts_transformations()

        test_results = []
        # Test 1: dim_warehouses uniqueness
        df1, _ = engine.query("SELECT COUNT(*) AS cnt, COUNT(DISTINCT warehouse_id) AS uniq FROM dim_warehouses;")
        t1_pass = bool(df1["cnt"].iloc[0] == df1["uniq"].iloc[0])
        test_results.append({"name": "test_unique_warehouse_id", "pass": t1_pass})

        # Test 2: stg_orders status accepted values
        df2, _ = engine.query("SELECT COUNT(*) AS invalid_cnt FROM stg_orders WHERE order_status NOT IN ('delivered', 'shipped', 'processing', 'canceled');")
        t2_pass = bool(df2["invalid_cnt"].iloc[0] == 0)
        test_results.append({"name": "test_accepted_values_order_status", "pass": t2_pass})

        # Test 3: fct_stockout_risks positive reorder qty
        df3, _ = engine.query("SELECT COUNT(*) AS neg_cnt FROM fct_stockout_risks WHERE recommended_reorder_qty < 0;")
        t3_pass = bool(df3["neg_cnt"].iloc[0] == 0)
        test_results.append({"name": "test_non_negative_reorder_qty", "pass": t3_pass})

        # Test 4: fct_delivery_performance valid percentage
        df4, _ = engine.query("SELECT COUNT(*) AS bad_pct FROM fct_delivery_performance WHERE on_time_delivery_rate_pct < 0 OR on_time_delivery_rate_pct > 100;")
        t4_pass = bool(df4["bad_pct"].iloc[0] == 0)
        test_results.append({"name": "test_valid_on_time_delivery_pct", "pass": t4_pass})

        engine.close()
        elapsed = (time.perf_counter() - t0) * 1000.0
        all_passed = all(t["pass"] for t in test_results)

        return {
            "status": "SUCCESS" if all_passed else "FAILED",
            "duration_ms": round(elapsed, 2),
            "total_dbt_tests": len(test_results),
            "passed_dbt_tests": sum(1 for t in test_results if t["pass"]),
            "tests": test_results,
        }

    def task_bigquery_sync(self) -> Dict[str, Any]:
        """Task 7: Google BigQuery external table manifest sync."""
        t0 = time.perf_counter()
        bq_mgr = BigQueryWarehouseManager()
        ddls = bq_mgr.generate_all_ddls()
        cost = bq_mgr.estimate_query_cost(estimated_bytes_scanned=250 * 1024 * 1024)
        elapsed = (time.perf_counter() - t0) * 1000.0

        return {
            "status": "SUCCESS",
            "duration_ms": round(elapsed, 2),
            "ddl_tables": list(ddls.keys()),
            "project_id": bq_mgr.project_id,
            "dataset_id": bq_mgr.dataset_id,
            "cost_estimation": cost,
        }

    def task_evaluate_alerts(self) -> Dict[str, Any]:
        """Task 8: Evaluate critical stockout alerts."""
        t0 = time.perf_counter()
        engine = DuckDBAnalyticsEngine(db_path=self.duckdb_path)
        engine.register_parquet_sources(raw_data_dir=self.raw_dir)
        engine.execute_staging_transformations()
        engine.execute_marts_transformations()
        summary = engine.get_stockout_summary()
        engine.close()
        elapsed = (time.perf_counter() - t0) * 1000.0

        return {
            "status": "SUCCESS",
            "duration_ms": round(elapsed, 2),
            "summary": summary,
        }

    def run_full_pipeline(self) -> Dict[str, Any]:
        """Executes all 8 tasks in topological order, mirroring Airflow execution."""
        task_dispatch = {
            "extract_and_stage_raw_data": self.task_ingest_raw,
            "validate_ingestion_data_quality": self.task_quality_gate,
            "sync_iceberg_snapshots": self.task_iceberg_sync,
            "run_dbt_staging_transformations": self.task_dbt_staging,
            "run_dbt_marts_transformations": self.task_dbt_marts,
            "test_dbt_data_models": self.task_dbt_tests,
            "sync_bigquery_manifest": self.task_bigquery_sync,
            "evaluate_stockout_risk_alerts": self.task_evaluate_alerts,
        }

        start_time = datetime.now(timezone.utc).isoformat()
        t0_total = time.perf_counter()
        results = []
        pipeline_success = True

        for task_id, desc in self.TASKS:
            fn = task_dispatch[task_id]
            try:
                task_output = fn()
                results.append({
                    "task_id": task_id,
                    "description": desc,
                    "status": task_output.get("status", "SUCCESS"),
                    "duration_ms": task_output.get("duration_ms", 0.0),
                    "output": task_output,
                })
                if task_output.get("status") == "FAILED":
                    pipeline_success = False
            except Exception as e:
                pipeline_success = False
                results.append({
                    "task_id": task_id,
                    "description": desc,
                    "status": "FAILED",
                    "error": str(e),
                })
                break

        total_elapsed_ms = (time.perf_counter() - t0_total) * 1000.0

        summary_manifest = {
            "pipeline_name": "logimesh_supply_chain_mesh",
            "start_time": start_time,
            "end_time": datetime.now(timezone.utc).isoformat(),
            "overall_status": "SUCCESS" if pipeline_success else "FAILED",
            "total_duration_ms": round(total_elapsed_ms, 2),
            "tasks_executed": len(results),
            "task_runs": results,
        }

        # Save manifest
        manifest_path = os.path.join(self.outputs_dir, "last_pipeline_run.json")
        with open(manifest_path, "w") as f:
            json.dump(summary_manifest, f, indent=2, default=str)

        return summary_manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="LogiMesh Pipeline Task Runner")
    parser.add_argument("--task", default="all", help="Task name or 'all'")
    args = parser.parse_args()

    runner = PipelineRunner()
    if args.task == "all":
        res = runner.run_full_pipeline()
        print(f"Pipeline executed: Status={res['overall_status']}, Duration={res['total_duration_ms']}ms")
        for t in res["task_runs"]:
            print(f"  [{t['status']}] {t['task_id']} ({t.get('duration_ms', 0)}ms)")
    else:
        print(f"Running individual task: {args.task}")
