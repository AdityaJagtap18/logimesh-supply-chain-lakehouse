"""Main Streamlit application for PRJ-02: LogiMesh.

Enterprise Supply Chain & Logistics Lakehouse Control Center.
"""

import os
import sys
import streamlit as st

# Ensure project root is in sys.path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from app.components.dag_monitor import render_dag_monitor
from app.components.dbt_lineage import render_dbt_lineage
from app.components.iceberg_explorer import render_iceberg_explorer
from app.components.quality_report import render_quality_report
from app.components.stockout_analytics import render_stockout_analytics
from src.warehouse.bigquery_manager import BigQueryWarehouseManager

st.set_page_config(
    page_title="LogiMesh — Supply Chain Lakehouse",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom header styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.2rem;
        font-weight: 700;
        color: #1E3A8A;
        margin-bottom: 0.2rem;
    }
    .sub-header {
        font-size: 1.05rem;
        color: #4B5563;
        margin-bottom: 1.5rem;
    }
</style>
""", unsafe_allow_html=True)

st.markdown('<div class="main-header">📦 LogiMesh: Supply Chain & Logistics Lakehouse</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-header">Multi-Tier Inventory Visibility, Apache Iceberg ACID Metadata, dbt Marts & Great Expectations Quality Gate</div>', unsafe_allow_html=True)

# Sidebar Navigation
st.sidebar.image("https://img.icons8.com/color/96/delivery.png", width=64)
st.sidebar.title("Navigation")
page = st.sidebar.radio(
    "Control Center Sections:",
    [
        "🚀 Airflow DAG Pipeline",
        "📦 Stockout Risk Analytics",
        "🛠️ dbt Models & Lineage",
        "🧊 Apache Iceberg Lakehouse",
        "🛡️ Great Expectations DQ",
        "☁️ Google BigQuery Manifest",
    ]
)

st.sidebar.divider()
st.sidebar.markdown("### Approved Stack")
st.sidebar.markdown("""
- **Airflow**: DAG Scheduling
- **dbt**: ELT Models & Schema Tests
- **Apache Iceberg**: ACID Lakehouse & Time Travel
- **DuckDB**: Fast In-Process OLAP
- **Great Expectations**: Data Contracts
- **Google BigQuery**: Cloud External Tables
- **SQL**: Window Functions & Reorder Math
""")

if page == "🚀 Airflow DAG Pipeline":
    render_dag_monitor(PROJECT_ROOT)
elif page == "📦 Stockout Risk Analytics":
    render_stockout_analytics(PROJECT_ROOT)
elif page == "🛠️ dbt Models & Lineage":
    render_dbt_lineage(PROJECT_ROOT)
elif page == "🧊 Apache Iceberg Lakehouse":
    render_iceberg_explorer(PROJECT_ROOT)
elif page == "🛡️ Great Expectations DQ":
    render_quality_report(PROJECT_ROOT)
elif page == "☁️ Google BigQuery Manifest":
    st.subheader("☁️ Google BigQuery External Table Manifest & Dialect Transpiler")
    st.caption("Zero-Copy Enterprise Cloud Integration Mapped to Apache Iceberg Metadata")

    bq_mgr = BigQueryWarehouseManager()
    ddls = bq_mgr.generate_all_ddls()

    bcol1, bcol2, bcol3 = st.columns(3)
    bcol1.metric("Target Project", bq_mgr.project_id)
    bcol2.metric("Target Dataset", bq_mgr.dataset_id)
    bcol3.metric("Multi-Region", bq_mgr.location)

    st.write("#### Generated External Table DDLs")
    tbl_choice = st.selectbox("Select Table DDL to inspect:", list(ddls.keys()))
    st.code(ddls[tbl_choice], language="sql")

    st.divider()
    st.write("#### BigQuery Query Cost Estimator")
    gb_scanned = st.slider("Estimated Data Scanned per Query (GB):", 1, 10000, 250)
    cost = bq_mgr.estimate_query_cost(gb_scanned * 1024 * 1024 * 1024)
    ecol1, ecol2, ecol3 = st.columns(3)
    ecol1.metric("TB Scanned", f"{cost['estimated_tb_scanned']:.4f} TB")
    ecol2.metric("Estimated Cost", f"${cost['estimated_cost_usd']:.4f}")
    ecol3.metric("Free Tier Status", "Eligible (<= 1 TB/mo)" if cost["free_tier_eligible"] else "Billed On-Demand")
