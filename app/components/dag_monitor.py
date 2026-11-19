"""Airflow DAG pipeline monitor component for LogiMesh."""

import json
import os
import streamlit as st
import pandas as pd
from src.orchestration.pipeline_runner import PipelineRunner


def render_dag_monitor(base_dir: str):
    st.subheader("⚡ Apache Airflow DAG Orchestration Monitor")
    st.caption("DAG ID: `logimesh_supply_chain_mesh` | Schedule: `@daily` | Catchup: `False`")

    col1, col2 = st.columns([2, 1])
    with col1:
        st.markdown("""
        **Pipeline Execution Topology:**
        ```
        [extract_and_stage_raw_data] ➔ [validate_ingestion_data_quality] ➔ [sync_iceberg_snapshots]
                                                                                  │
                                                                                  ▼
        [test_dbt_data_models] ➔ [run_dbt_marts_transformations] ➔ [run_dbt_staging_transformations]
                  │
                  ├─➔ [sync_bigquery_manifest]
                  └─➔ [evaluate_stockout_risk_alerts]
        ```
        """)
    with col2:
        st.markdown("#### Trigger DAG Run")
        if st.button("🚀 Execute Airflow Pipeline", type="primary", use_container_width=True):
            with st.spinner("Executing Airflow DAG tasks sequentially..."):
                runner = PipelineRunner(base_dir=base_dir)
                res = runner.run_full_pipeline()
                if res["overall_status"] == "SUCCESS":
                    st.success(f"Pipeline executed successfully in {res['total_duration_ms']:.1f} ms!")
                else:
                    st.error("Pipeline run failed.")

    # Read latest execution manifest
    manifest_path = os.path.join(base_dir, "outputs", "last_pipeline_run.json")
    if os.path.exists(manifest_path):
        try:
            with open(manifest_path, "r") as f:
                manifest = json.load(f)

            st.divider()
            mcol1, mcol2, mcol3, mcol4 = st.columns(4)
            mcol1.metric("Status", manifest.get("overall_status", "UNKNOWN"))
            mcol2.metric("Total Duration", f"{manifest.get('total_duration_ms', 0):.1f} ms")
            mcol3.metric("Tasks Completed", f"{manifest.get('tasks_executed', 0)} / 8")
            mcol4.metric("Engine", "DuckDB + PyIceberg")

            st.write("#### Task Execution Run Log")
            task_data = []
            for t in manifest.get("task_runs", []):
                task_data.append({
                    "Task ID": t["task_id"],
                    "Description": t["description"],
                    "Status": "✅ " + t["status"] if t["status"] == "SUCCESS" else "❌ " + t["status"],
                    "Duration (ms)": t.get("duration_ms", 0.0),
                })
            st.dataframe(pd.DataFrame(task_data), use_container_width=True, hide_index=True)

            with st.expander("🔍 Inspect Full Pipeline JSON Manifest"):
                st.json(manifest)
        except Exception as e:
            st.warning(f"Could not parse manifest: {e}")
    else:
        st.info("No prior pipeline runs found. Click 'Execute Airflow Pipeline' above to initiate.")
