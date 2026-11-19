"""Supply chain stockout analytics and risk control center component."""

import os
import streamlit as st
import pandas as pd
from src.core.duckdb_engine import DuckDBAnalyticsEngine


def render_stockout_analytics(base_dir: str):
    st.subheader("📦 Real-Time Supply Chain & Stockout Risk Center")
    st.caption("OLAP Windowed Demand Analytics | Days of Supply (DoS) | Dynamic Safety Stock Reorders")

    db_path = os.path.join(base_dir, "data", "duckdb", "logimesh.duckdb")
    raw_dir = os.path.join(base_dir, "data", "raw")

    if not os.path.exists(db_path):
        st.warning("DuckDB database not found. Please run the Airflow pipeline first.")
        return

    engine = DuckDBAnalyticsEngine(db_path=db_path)
    engine.register_parquet_sources(raw_data_dir=raw_dir)
    engine.execute_staging_transformations()
    engine.execute_marts_transformations()

    summary = engine.get_stockout_summary()

    # KPI Top Bar
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Monitored SKU Positions", summary["total_monitored_skus"])
    c2.metric("Critical Stockouts (Stock = 0)", summary["critical_stockouts"], delta=f"{summary['critical_percentage']}%", delta_color="inverse")
    c3.metric("Capital at Risk ($ USD)", f"${summary['total_capital_at_risk_usd']:,.2f}")
    c4.metric("Healthy Inventory SKUs", summary["healthy_skus"])

    st.divider()

    # Query full risk mart
    df_risks, q_time = engine.query("""
        SELECT
            warehouse_name,
            region,
            product_id,
            product_name,
            category,
            unit_price,
            stock_on_hand,
            safety_stock_level,
            reorder_point,
            lead_time_days,
            avg_daily_demand_30d,
            days_of_supply,
            recommended_reorder_qty,
            stockout_capital_at_risk,
            risk_score,
            stockout_alert_status
        FROM fct_stockout_risks
        ORDER BY risk_score DESC, stockout_capital_at_risk DESC;
    """)

    # Filter Controls
    fcol1, fcol2, fcol3 = st.columns(3)
    with fcol1:
        wh_options = ["All Warehouses"] + list(df_risks["warehouse_name"].unique())
        selected_wh = st.selectbox("Filter Warehouse:", wh_options)
    with fcol2:
        cat_options = ["All Categories"] + list(df_risks["category"].unique())
        selected_cat = st.selectbox("Filter Category:", cat_options)
    with fcol3:
        status_options = ["All Statuses"] + list(df_risks["stockout_alert_status"].unique())
        selected_status = st.selectbox("Filter Alert Status:", status_options)

    filtered_df = df_risks.copy()
    if selected_wh != "All Warehouses":
        filtered_df = filtered_df[filtered_df["warehouse_name"] == selected_wh]
    if selected_cat != "All Categories":
        filtered_df = filtered_df[filtered_df["category"] == selected_cat]
    if selected_status != "All Statuses":
        filtered_df = filtered_df[filtered_df["stockout_alert_status"] == selected_status]

    st.write(f"Showing **{len(filtered_df)} SKUs** matching criteria (Query executed in {q_time:.2f} ms):")

    # Display high-risk items
    def highlight_risk(val):
        if val == "CRITICAL_STOCKOUT":
            return "background-color: #ffcccc; color: #990000; font-weight: bold;"
        elif val == "HIGH_IMMINENT_STOCKOUT":
            return "background-color: #ffe6cc; color: #cc6600; font-weight: bold;"
        elif val == "MODERATE_SAFETY_BREACH":
            return "background-color: #ffffcc; color: #999900;"
        return ""

    display_cols = [
        "warehouse_name", "product_name", "category", "unit_price",
        "stock_on_hand", "safety_stock_level", "avg_daily_demand_30d",
        "days_of_supply", "recommended_reorder_qty", "stockout_capital_at_risk",
        "risk_score", "stockout_alert_status"
    ]
    st.dataframe(
        filtered_df[display_cols],
        use_container_width=True,
        hide_index=True
    )

    st.divider()
    st.write("#### 🚚 Warehouse On-Time Delivery (OTD) Benchmarks")
    df_perf, _ = engine.query("""
        SELECT
            warehouse_name,
            region,
            total_orders,
            on_time_orders,
            avg_transit_days,
            avg_delay_days,
            on_time_delivery_rate_pct
        FROM fct_delivery_performance
        ORDER BY on_time_delivery_rate_pct DESC;
    """)
    st.dataframe(df_perf, use_container_width=True, hide_index=True)

    engine.close()
