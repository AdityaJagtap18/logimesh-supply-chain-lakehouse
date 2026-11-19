"""Apache Iceberg table explorer and Time Travel inspector component."""

import os
import streamlit as st
import pandas as pd
from src.core.iceberg_manager import IcebergTableManager


def render_iceberg_explorer(base_dir: str):
    st.subheader("🧊 Apache Iceberg Lakehouse Inspector")
    st.caption("Open Lakehouse Format: ACID Snapshots | Hidden Partitioning | Schema Evolution | Time Travel")

    iceberg_dir = os.path.join(base_dir, "data", "iceberg")
    mgr = IcebergTableManager(warehouse_path=iceberg_dir)

    # Discover available tables
    if not os.path.exists(iceberg_dir):
        st.info("Iceberg warehouse directory is empty. Run the pipeline to register tables.")
        return

    tables = [d for d in os.listdir(iceberg_dir) if os.path.isdir(os.path.join(iceberg_dir, d))]
    if not tables:
        st.info("No Iceberg tables registered yet. Trigger the Airflow pipeline to initialize.")
        return

    selected_table = st.selectbox("Select Iceberg Table:", tables)
    meta = mgr.get_table_metadata(selected_table)

    if not meta:
        st.warning("No metadata JSON found for selected table.")
        return

    # Metadata overview
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Format Version", f"v{meta.get('format_version', 2)}")
    col2.metric("Current Snapshot ID", str(meta.get("current_snapshot_id", "N/A"))[:10] + "...")
    col3.metric("Total Snapshots", len(meta.get("snapshots", [])))
    col4.metric("Format Codec", meta.get("properties", {}).get("write.format.default", "parquet"))

    st.write("#### Schema Definition")
    schema_df = pd.DataFrame([
        {"Column": col, "Data Type": dtype}
        for col, dtype in meta.get("schema", {}).items()
    ])
    st.dataframe(schema_df, use_container_width=True, hide_index=True)

    st.divider()
    st.write("#### ⏳ Historical Snapshot Log & Time Travel Replay")
    snapshots = meta.get("snapshots", [])

    if snapshots:
        snap_df = pd.DataFrame([
            {
                "Snapshot ID": s["snapshot_id"],
                "Parent ID": s.get("parent_snapshot_id", "None"),
                "Operation": s.get("operation", "append"),
                "Timestamp (UTC)": s.get("timestamp_iso", ""),
                "Records Added": s.get("summary", {}).get("added_records", "N/A"),
                "Data File": s.get("data_file", "none"),
            }
            for s in snapshots
        ])
        st.dataframe(snap_df, use_container_width=True, hide_index=True)

        # Time Travel Selector
        snap_ids = [s["snapshot_id"] for s in snapshots if not s.get("data_file", "").startswith("none")]
        if snap_ids:
            target_snap = st.selectbox("Select Snapshot for Time Travel Query:", snap_ids)
            if st.button("Query Historical Snapshot Data"):
                with st.spinner("Executing Time Travel query on Iceberg catalog..."):
                    df_snap = mgr.load_as_snapshot(selected_table, target_snap)
                    st.success(f"Snapshot `{target_snap}` loaded: {len(df_snap)} rows found.")
                    st.dataframe(df_snap.head(15), use_container_width=True)

    # Schema evolution demo
    with st.expander("➕ Demonstrate Schema Evolution (Add Column Without Data Rewrites)"):
        new_col = st.text_input("New Column Name:", value="carrier_performance_score")
        col_type = st.selectbox("Column Type:", ["string", "integer", "float"])
        default_val = st.text_input("Default Value:", value="A+")
        if st.button("Apply Schema Evolution"):
            try:
                res = mgr.evolve_schema_add_column(selected_table, new_col, default_val, col_type)
                st.success(f"Column `{new_col}` added to Iceberg schema! Snapshot registered.")
                st.rerun()
            except Exception as e:
                st.error(f"Evolution failed: {e}")
