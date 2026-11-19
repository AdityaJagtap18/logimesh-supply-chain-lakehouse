# Architectural Specification — PRJ-02: LogiMesh

## 1. System Vision & Business Context

Global supply chains are subjected to continuous disruptions: supplier lead-time variance, shipping container transit delays, regional warehouse capacity constraints, and sudden demand spikes. Without an integrated, transactional Lakehouse architecture, inventory records remain siloed across disparate ERP systems, resulting in:
- Blind stockouts on high-margin, high-velocity SKUs.
- Severe working capital lock-up in slow-moving or obsolete inventory.
- High carrier expedited freight costs due to reactive replenishment.

**LogiMesh** unifies orders, carrier shipments, warehouse capacities, and SKU inventory levels into an open-standard, ACID-compliant Lakehouse architecture.

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

## 2. Technology Assignment & Architectural Boundaries

| Tool | Assigned Role | Justification |
|---|---|---|
| **Apache Airflow** | Workflow Orchestration | Schedules daily batch ingestion, quality gate evaluation, dbt transformations, and BigQuery manifest sync with dependency graphs and retry logic. |
| **dbt** | Transformation Modeling | Version-controlled SQL modeling separating raw ingestion from staging views and dimensional/fact marts. |
| **Apache Iceberg** | Open Table Format | ACID transaction guarantees, schema evolution without rewriting data files, hidden partitioning, and historical Time Travel. |
| **Google BigQuery** | Cloud Data Warehouse | Enterprise external table definitions reading Iceberg metadata directly with date partitioning and SKU clustering. |
| **DuckDB** | In-Process OLAP Engine | Fast sub-second execution of analytical window functions, Days of Supply (DoS), and local dbt transformations with zero infrastructure cost. |
| **Great Expectations** | Data Quality Enforcement | Formal assertion contracts guaranteeing data integrity before downstream modeling. |
| **SQL** | Analytical Transformation | Complex CTEs, moving averages, and lead-time replenishment calculations. |
| *Streamlit* | Operations UI | Interactive control center for Airflow DAGs, stockout risk heatmaps, Iceberg time travel, and DQ scorecards. |

---

## 3. Stockout Risk Formulation

The real-time risk engine computes:

1. **Average Daily Demand (30-day Window)**:
   $$\bar{D}_{30} = \frac{1}{30} \sum_{t=1}^{30} \text{DailyQuantitySold}_t$$

2. **Days of Supply (DoS)**:
   $$\text{DoS} = \frac{\text{StockOnHand}}{\bar{D}_{30}}$$

3. **Recommended Reorder Quantity**:
   $$\text{ReorderQty} = \max\left(0, (\text{LeadTimeDays} \times \bar{D}_{30}) + \text{SafetyStock} - \text{StockOnHand}\right)$$

4. **Capital at Risk ($ USD)**:
   $$\text{CapitalAtRisk} = \text{ReorderQty} \times \text{UnitPrice}$$

5. **Risk Classification**:
   - `CRITICAL_STOCKOUT`: $\text{StockOnHand} = 0$
   - `HIGH_IMMINENT_STOCKOUT`: $\text{DoS} \le \text{LeadTimeDays} \times 0.5$
   - `MODERATE_SAFETY_BREACH`: $\text{StockOnHand} \le \text{SafetyStock}$
   - `LOW_REORDER_TRIGGERED`: $\text{StockOnHand} \le \text{ReorderPoint}$
   - `HEALTHY`: $\text{StockOnHand} > \text{ReorderPoint}$
