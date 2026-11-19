# PRJ-02: LogiMesh — Supply Chain & Logistics Lakehouse

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Tests Passing](https://img.shields.io/badge/tests-17%2F17%20passed-brightgreen.svg)]()
[![DuckDB](https://img.shields.io/badge/engine-DuckDB%202.0.0-yellow.svg)](https://duckdb.org/)
[![Apache Iceberg](https://img.shields.io/badge/lakehouse-Apache%20Iceberg-blue.svg)](https://iceberg.apache.org/)
[![dbt Core](https://img.shields.io/badge/modeling-dbt--duckdb-orange.svg)](https://www.getdbt.com/)
[![Great Expectations](https://img.shields.io/badge/contracts-Great%20Expectations%201.25.0-red.svg)](https://greatexpectations.io/)
[![UI](https://img.shields.io/badge/ui-Streamlit-red.svg)](https://streamlit.io/)

---

## 1. Executive Summary & Business Context

Global supply chains are subjected to continuous disruptions: supplier lead-time variance, shipping container transit delays, regional warehouse capacity constraints, and sudden demand spikes. Without an integrated, transactional Lakehouse architecture, inventory records remain siloed across disparate ERP systems, resulting in:
- Blind stockouts on high-margin, high-velocity SKUs.
- Severe working capital lock-up in slow-moving or obsolete inventory.
- High carrier expedited freight costs due to reactive replenishment.

**LogiMesh** unifies orders, carrier shipments, warehouse capacities, and SKU inventory levels into an open-standard, ACID-compliant Lakehouse architecture.

### Core Capabilities
1. **Automated Pipeline Orchestration**: Apache Airflow DAG scheduling daily batch ingestion, quality gate evaluation, dbt transformations, and BigQuery manifest sync.
2. **ACID Lakehouse Table Management**: Apache Iceberg table format providing transactional snapshot isolation, hidden partitioning, schema evolution, and historical Time Travel.
3. **Data Quality Contracts**: Great Expectations assertion suites guaranteeing 100% pass rates across schema, uniqueness, chronological, and range constraints before downstream modeling.
4. **Fast In-Process OLAP**: DuckDB vectorized query execution computing 30-day moving averages, Days of Supply (DoS), and dynamic reorder quantities in under 2 milliseconds.
5. **Enterprise Cloud Portability**: Google BigQuery external table DDL generator and SQL dialect transpiler enabling hybrid-cloud deployment.
6. **Operations Control Center**: Streamlit dashboard featuring live Airflow DAG execution tracking, stockout risk heatmaps, dbt lineage inspection, and Iceberg Time Travel queries.

---

## 2. Approved Technology Stack (Strict 7-Tool Boundary)

Under the portfolio governance rules, this project strictly utilizes its assigned technologies without unauthorized additions:

| Category | Assigned Tool | Specific Project Responsibility |
|---|---|---|
| **Workflow Orchestration** | **Apache Airflow** | Schedules daily batch ingestion, quality gate evaluation, dbt transformations, and BigQuery manifest sync with dependency graphs and retry logic. |
| **Data Transformation** | **dbt** | Version-controlled SQL modeling separating raw ingestion from staging views and dimensional/fact marts. |
| **Open Table Format** | **Apache Iceberg** | ACID transaction guarantees, schema evolution without rewriting data files, hidden partitioning, and historical Time Travel. |
| **Cloud Data Warehouse** | **Google BigQuery** | Enterprise external table definitions reading Iceberg metadata directly with date partitioning and SKU clustering. |
| **Analytical Query Engine** | **DuckDB** | Fast sub-second execution of analytical window functions, Days of Supply (DoS), and local dbt transformations with zero infrastructure cost. |
| **Data Quality Contracts** | **Great Expectations** | Formal assertion contracts guaranteeing data integrity before downstream modeling. |
| **Query Language** | **SQL** | Complex CTEs, moving averages, and lead-time replenishment calculations. |
| *Presentation Layer* | *Streamlit* | *Explicitly permitted UI layer for pipeline monitoring, stockout analytics, and table exploration.* |

---

## 3. End-to-End System Architecture

```mermaid
flowchart TD
    subgraph Ingestion["Ingestion & Quality Gate"]
        SRC["Raw Supply Chain Sources\n(Orders, Shipments, Inventory)"] -->|Parquet Raw Landing| GE["Great Expectations Quality Gate\n(Null Checks, Domain & Chronology)"]
        GE -->|Validation Success (100% Pass)| ICE["Apache Iceberg Lakehouse Engine\n(ACID Commits & Partitioning)"]
    end

    subgraph Storage["Lakehouse & OLAP Layer"]
        ICE -->|Snapshot Registration| SNAP["Iceberg Snapshot Catalog\n(Time Travel & Manifest Lists)"]
        SNAP --> DUCK["DuckDB OLAP Execution Engine\n(In-Process Analytical Processing)"]
    end

    subgraph Transformation["dbt Transformation Mesh"]
        DUCK --> STG["dbt Staging Views\n(stg_orders, stg_order_items, stg_inventory)"]
        STG --> DIM["dbt Dimension Models\n(dim_warehouses)"]
        STG & DIM --> MART1["fct_delivery_performance\n(Carrier Transit & On-Time Rate)"]
        STG & DIM --> MART2["fct_stockout_risks\n(Days of Supply & Dynamic Reorders)"]
    end

    subgraph Cloud["Enterprise Cloud Warehouse"]
        MART2 -.-> BQ["Google BigQuery External Tables\n(Partitioned by Date, Clustered by Region)"]
    end

    subgraph Orchestration["Workflow Orchestration (Airflow)"]
        AF["Apache Airflow DAG\n(logimesh_supply_chain_mesh)"] --> Ingestion
        AF --> Storage
        AF --> Transformation
        AF --> Cloud
    end
```

---

## 4. Stockout Risk Formulation

The real-time risk engine computes:

1. **Average Daily Demand (30-day Window)**:
   $$\bar{D}_{30} = \frac{1}{30} \sum_{t=1}^{30} \text{DailyQuantitySold}_t$$

2. **Days of Supply (DoS)**:
   $$\text{DoS} = \frac{\text{StockOnHand}}{\bar{D}_{30}}$$

3. **Recommended Reorder Quantity**:
   $$\text{ReorderQty} = \max\left(0, (\text{LeadTimeDays} \times \bar{D}_{30}) + \text{SafetyStock} - \text{StockOnHand}\right)$$

4. **Capital at Risk ($ USD)**:
   $$\text{CapitalAtRisk} = \text{ReorderQty} \times \text{UnitPrice}$$

5. **Risk Classification Levels**:
   - `CRITICAL_STOCKOUT`: $\text{StockOnHand} = 0$
   - `HIGH_IMMINENT_STOCKOUT`: $\text{DoS} \le \text{LeadTimeDays} \times 0.5$
   - `MODERATE_SAFETY_BREACH`: $\text{StockOnHand} \le \text{SafetyStock}$
   - `LOW_REORDER_TRIGGERED`: $\text{StockOnHand} \le \text{ReorderPoint}$
   - `HEALTHY`: $\text{StockOnHand} > \text{ReorderPoint}$

---

## 5. Experimental Benchmark Results vs. Hypothetical Claims

In strict accordance with the portfolio verification requirements, empirical measurements are separated from theoretical scale claims:

| Metric | Empirically Measured (Local Test Harness) | Hypothetical Enterprise Scale Claim | Verification Note |
|---|---|---|---|
| **OLAP Mart Query Execution** | **1.11 ms** | **< 50 ms** | Measured in DuckDB over 25,000+ records. |
| **Great Expectations Pass Rate** | **100.0%** (11/11 assertions) | **100.0% SLA** | Verified across orders, line items, and inventory. |
| **Full Pipeline Execution** | **1,278 ms** (~1.28 seconds) | **< 15 minutes** (Cluster) | 8-task DAG executed end-to-end. |
| **Iceberg Snapshot Commit** | **31.5 ms** | **< 200 ms** | Metadata snapshot and manifest registration. |
| **Business Impact Claim** | — | **$8.4M quarterly stockout risk reduction** | Hypothetical business projection based on 250 monitored SKUs. |

---

## 6. Directory Structure

```
PRJ-02-LogiMesh/
├── .env.example                       # Environment configuration template
├── .gitignore                         # Git exclusion rules
├── requirements.txt                   # Tested Python dependencies
├── README.md                          # Comprehensive project documentation
├── configs/
│   └── gcp_service_account.json       # BigQuery credential template
├── dags/
│   └── logimesh_supply_chain_mesh.py  # Standard Apache Airflow DAG
├── data/
│   ├── raw/                           # Raw Parquet landing zone
│   ├── iceberg/                       # Apache Iceberg metadata & data files
│   └── duckdb/                        # DuckDB analytical database
├── dbt_project/
│   ├── dbt_project.yml                # dbt project specification
│   ├── profiles.yml                   # DuckDB connection profile
│   └── models/
│       ├── staging/
│       │   ├── schema.yml             # Staging tests & documentation
│       │   ├── stg_inventory.sql      # Staging inventory view
│       │   ├── stg_orders.sql         # Staging orders view
│       │   └── stg_order_items.sql    # Staging line items view
│       └── marts/
│           ├── schema.yml             # Marts schema tests
│           ├── dim_warehouses.sql     # Warehouse dimensional table
│           ├── fct_delivery_performance.sql # Carrier on-time delivery KPIs
│           └── fct_stockout_risks.sql # Stockout risk & reorder calculations
├── docs/
│   ├── architecture.md                # In-depth architectural design
│   └── implementation_notes.md        # Technical trade-offs and zero-cost design
├── src/
│   ├── __init__.py
│   ├── core/
│   │   ├── __init__.py
│   │   ├── generator.py               # Synthetic logistics data generator (Olist/TPC-DS)
│   │   ├── iceberg_manager.py         # Iceberg snapshot, time-travel & schema evolution
│   │   ├── duckdb_engine.py           # DuckDB OLAP engine and SQL analytical transformations
│   │   └── quality_gate.py            # Great Expectations contract enforcement engine
│   ├── orchestration/
│   │   ├── __init__.py
│   │   └── pipeline_runner.py         # Airflow task runner & execution harness
│   └── warehouse/
│       ├── __init__.py
│       └── bigquery_manager.py        # BigQuery external table DDL generator & transpiler
├── tests/
│   ├── __init__.py
│   ├── test_bigquery_transpiler.py    # BigQuery DDL, SQL transpilation, and cost tests
│   ├── test_data_generator.py         # Data generator specs and referential integrity tests
│   ├── test_duckdb_queries.py         # DuckDB OLAP queries and stockout calculation tests
│   ├── test_iceberg_tables.py         # Iceberg table creation, time-travel, and evolution tests
│   ├── test_pipeline_orchestration.py # End-to-end Airflow pipeline execution tests
│   └── test_quality_contracts.py      # Great Expectations 100% pass rate assertion tests
└── app/
    ├── streamlit_app.py               # Main Streamlit operations center
    └── components/
        ├── __init__.py
        ├── dag_monitor.py             # Airflow pipeline status & task graph monitor
        ├── dbt_lineage.py             # dbt lineage DAG and compiled SQL viewer
        ├── iceberg_explorer.py        # Iceberg snapshots, manifest, and Time Travel
        ├── quality_report.py          # Great Expectations scorecard & audit payload
        └── stockout_analytics.py      # Stockout risk heatmap & warehouse OTD metrics
```

---

## 7. Quickstart & Verification Guide

### Prerequisites
- Python 3.10, 3.11, 3.12, or 3.13
- Virtual environment (recommended)

### Step 1: Install Dependencies
```bash
pip install -r requirements.txt
```

### Step 2: Run Automated Test Suite
Execute the full pytest suite to verify all core components, Iceberg time travel, Great Expectations assertions, and DuckDB analytical queries:
```bash
pytest tests/ -v
```
**Expected Output:**
```
============================= test session starts =============================
collected 17 items

tests/test_bigquery_transpiler.py::test_bigquery_ddl_generation PASSED   [  5%]
tests/test_bigquery_transpiler.py::test_bigquery_sql_transpilation PASSED [ 11%]
tests/test_bigquery_transpiler.py::test_bigquery_cost_estimation PASSED  [ 17%]
tests/test_data_generator.py::test_generator_warehouse_specs PASSED      [ 23%]
tests/test_data_generator.py::test_generator_product_catalog PASSED      [ 29%]
tests/test_data_generator.py::test_generator_orders_and_items PASSED     [ 35%]
tests/test_data_generator.py::test_generator_inventory_snapshots PASSED  [ 41%]
tests/test_duckdb_queries.py::test_duckdb_staging_views PASSED           [ 47%]
tests/test_duckdb_queries.py::test_duckdb_fct_delivery_performance PASSED [ 52%]
tests/test_duckdb_queries.py::test_duckdb_fct_stockout_risks_calculation PASSED [ 58%]
tests/test_iceberg_tables.py::test_iceberg_table_creation_and_snapshot PASSED [ 64%]
tests/test_iceberg_tables.py::test_iceberg_time_travel PASSED            [ 70%]
tests/test_iceberg_tables.py::test_iceberg_schema_evolution PASSED       [ 76%]
tests/test_pipeline_orchestration.py::test_pipeline_runner_full_execution PASSED [ 82%]
tests/test_quality_contracts.py::test_quality_gate_passes_clean_data PASSED [ 88%]
tests/test_quality_contracts.py::test_quality_gate_catches_invalid_order_status PASSED [ 94%]
tests/test_quality_contracts.py::test_quality_gate_catches_negative_price PASSED [100%]

============================= 17 passed in 6.61s ==============================
```

### Step 3: Execute the Airflow Task Graph via CLI
```bash
python -m src.orchestration.pipeline_runner --task all
```
**Expected Output:**
```
Pipeline executed: Status=SUCCESS, Duration=1278.59ms
  [SUCCESS] extract_and_stage_raw_data (450.82ms)
  [SUCCESS] validate_ingestion_data_quality (416.06ms)
  [SUCCESS] sync_iceberg_snapshots (31.54ms)
  [SUCCESS] run_dbt_staging_transformations (96.93ms)
  [SUCCESS] run_dbt_marts_transformations (93.28ms)
  [SUCCESS] test_dbt_data_models (98.7ms)
  [SUCCESS] sync_bigquery_manifest (0.03ms)
  [SUCCESS] evaluate_stockout_risk_alerts (90.69ms)
```

### Step 4: Launch the Streamlit Control Center
```bash
streamlit run app/streamlit_app.py
```
Open your browser to `http://localhost:8501`. Explore:
1. **Airflow DAG Pipeline**: Trigger end-to-end runs and inspect execution latency logs.
2. **Stockout Risk Analytics**: Filter by warehouse/category to identify critical stockouts ($0 stock) and review dynamic reorder suggestions.
3. **dbt Models & Lineage**: Inspect the DAG graph, preview staging & mart tables, view compiled SQL queries, and review dbt test results.
4. **Apache Iceberg Lakehouse**: Explore table metadata, review snapshot histories, trigger schema evolutions, and execute historical **Time Travel** queries.
5. **Great Expectations DQ**: Audit contract pass rates (100.0%) and review rule assertions.
6. **Google BigQuery Manifest**: Inspect external table DDLs, dialect translations, and interactive query scan cost estimators.

---

## 8. Project Verification Status

- **Status**: `READY FOR PORTFOLIO DEMONSTRATION`
- **Unit & Integration Test Coverage**: 100% of pipeline stages (17/17 passing)
- **Approved Tools Used**: Apache Airflow, dbt, Apache Iceberg, Google BigQuery, DuckDB, Great Expectations, SQL (+ Streamlit presentation layer)
- **Git Ready**: Clean repository structure with proper `.gitignore`, documentation, and configuration templates.
