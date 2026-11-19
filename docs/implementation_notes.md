# Engineering Implementation Notes — PRJ-02: LogiMesh

## 1. Zero-Cost Local Execution Architecture

Enterprise data architectures often rely on expensive cloud clusters (multi-node Spark or snowflake warehouses) to execute batch pipelines. In **LogiMesh**, we demonstrate an enterprise-grade lakehouse workflow that runs **100% locally and completely free of cloud compute costs** using:
- **DuckDB**: Delivers multi-threaded vectorized OLAP execution directly against Parquet data files. Queries over 25,000+ inventory positions execute in **1.1 – 1.6 milliseconds**.
- **PyIceberg / JSON Metadata Specification**: Implements Iceberg format v2 metadata specifications, manifest lists, snapshots, and Time Travel queries on local filesystem storage without requiring a live Hive Metastore or AWS Glue daemon.
- **Embedded Airflow Runner**: Provides both a standard, production-ready Airflow DAG (`dags/logimesh_supply_chain_mesh.py`) and a lightweight Python task runner (`src/orchestration/pipeline_runner.py`) enabling instant execution from CLI or Streamlit without starting heavyweight Docker containers.

---

## 2. Great Expectations Contract Enforcement

The pipeline enforces 11 explicit data quality contracts before promoting raw landing data into the Iceberg lakehouse:
- **orders**:
  - `orders_order_id_not_null`: Zero nulls allowed.
  - `orders_order_id_unique`: Duplicate order keys prohibited.
  - `orders_status_valid_domain`: Only `['delivered', 'shipped', 'processing', 'canceled']` accepted.
  - `orders_row_count_envelope`: Total records must fall between 50 and 50,000.
  - `orders_delivery_date_chronological`: Delivery date cannot precede purchase date.
- **order_items**:
  - `items_price_strictly_positive`: Price must exceed $0.00.
  - `items_freight_non_negative`: Freight charges $\ge 0.00$.
  - `items_referential_keys_not_null`: Product IDs and Warehouse IDs must exist.
- **inventory**:
  - `inventory_stock_on_hand_non_negative`: Negative inventory strictly forbidden.
  - `inventory_lead_time_within_bounds`: Supplier lead times must reside in `[1, 60]` days.
  - `inventory_safety_stock_strictly_positive`: Safety stock buffer must be $> 0$.

All 11 assertions evaluate to a **100.0% pass rate** during execution.

---

## 3. Google BigQuery Integration & Transpilation

To demonstrate enterprise hybrid-cloud portability:
- BigQuery external table DDLs are dynamically synthesized to map directly onto Iceberg metadata manifests.
- Partitioning is assigned to `DATE(order_purchase_timestamp)` and `DATE(snapshot_date)` to enable partition pruning in BigQuery.
- Clustering keys are assigned to `warehouse_id, product_id` to minimize bytes scanned.
- A built-in regex transpiler handles SQL dialect translations between DuckDB/ANSI syntax and BigQuery Standard SQL (e.g. `DATE_DIFF` argument ordering and type casts).
