"""DuckDB in-process OLAP engine and SQL analytical transformation pipeline for LogiMesh.

Implements high-performance analytical queries, window functions, safety stock
reorder modeling, warehouse delivery KPIs, and stockout risk calculations.
"""

from datetime import datetime, timezone
import os
import time
from typing import Any, Dict, List, Optional, Tuple
import duckdb
import pandas as pd


class DuckDBAnalyticsEngine:
    """Analytical query engine powered by DuckDB with embedded SQL transformations."""

    def __init__(self, db_path: str = "data/duckdb/logimesh.duckdb"):
        self.db_path = db_path
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        self.con = duckdb.connect(self.db_path)
        self._init_extensions()

    def _init_extensions(self):
        """Initializes DuckDB configurations."""
        self.con.execute("PRAGMA threads=4;")
        self.con.execute("PRAGMA memory_limit='2GB';")

    def register_parquet_sources(self, raw_data_dir: str = "data/raw"):
        """Registers the raw Parquet datasets as views/tables in DuckDB."""
        tables = ["orders", "order_items", "inventory", "warehouses", "products"]
        for tbl in tables:
            path = os.path.join(raw_data_dir, f"{tbl}.parquet")
            if os.path.exists(path):
                # Replace backslashes with forward slashes for DuckDB path parsing
                norm_path = path.replace("\\", "/")
                self.con.execute(f"CREATE OR REPLACE VIEW raw_{tbl} AS SELECT * FROM read_parquet('{norm_path}');")

    def execute_staging_transformations(self):
        """Creates cleansed staging views with standardized dates and calculated metrics."""
        # 1. stg_orders
        self.con.execute("""
            CREATE OR REPLACE VIEW stg_orders AS
            SELECT
                order_id,
                customer_id,
                order_status,
                CAST(order_purchase_timestamp AS TIMESTAMP) AS purchase_timestamp,
                CAST(order_estimated_delivery_date AS TIMESTAMP) AS estimated_delivery_timestamp,
                CAST(order_delivered_customer_date AS TIMESTAMP) AS delivered_timestamp,
                CASE 
                    WHEN order_delivered_customer_date IS NOT NULL 
                    THEN DATE_DIFF('day', CAST(order_purchase_timestamp AS TIMESTAMP), CAST(order_delivered_customer_date AS TIMESTAMP))
                    ELSE NULL
                END AS actual_transit_days,
                CASE
                    WHEN order_delivered_customer_date IS NOT NULL AND order_estimated_delivery_date IS NOT NULL
                    THEN GREATEST(0, DATE_DIFF('day', CAST(order_estimated_delivery_date AS TIMESTAMP), CAST(order_delivered_customer_date AS TIMESTAMP)))
                    ELSE 0
                END AS carrier_delay_days,
                CASE
                    WHEN order_delivered_customer_date IS NOT NULL AND order_estimated_delivery_date IS NOT NULL
                    THEN CASE WHEN CAST(order_delivered_customer_date AS TIMESTAMP) <= CAST(order_estimated_delivery_date AS TIMESTAMP) THEN 1 ELSE 0 END
                    ELSE NULL
                END AS is_on_time
            FROM raw_orders;
        """)

        # 2. stg_order_items
        self.con.execute("""
            CREATE OR REPLACE VIEW stg_order_items AS
            SELECT
                order_id,
                item_id,
                product_id,
                warehouse_id,
                quantity,
                price,
                freight_value,
                ROUND(quantity * price, 2) AS line_item_subtotal,
                ROUND((quantity * price) + freight_value, 2) AS line_item_total
            FROM raw_order_items;
        """)

        # 3. stg_inventory
        self.con.execute("""
            CREATE OR REPLACE VIEW stg_inventory AS
            SELECT
                CAST(snapshot_date AS DATE) AS snapshot_date,
                warehouse_id,
                product_id,
                stock_on_hand,
                daily_demand,
                safety_stock_level,
                reorder_point,
                lead_time_days,
                CASE WHEN stock_on_hand <= safety_stock_level THEN 1 ELSE 0 END AS is_below_safety_stock,
                CASE WHEN stock_on_hand = 0 THEN 1 ELSE 0 END AS is_stockout
            FROM raw_inventory;
        """)

    def execute_marts_transformations(self):
        """Creates dimensional tables and aggregated analytical marts."""
        # 1. dim_warehouses
        self.con.execute("""
            CREATE OR REPLACE TABLE dim_warehouses AS
            SELECT
                warehouse_id,
                warehouse_name,
                region,
                capacity_sqft,
                operating_cost_per_day
            FROM raw_warehouses;
        """)

        # 2. fct_delivery_performance
        self.con.execute("""
            CREATE OR REPLACE TABLE fct_delivery_performance AS
            SELECT
                i.warehouse_id,
                w.warehouse_name,
                w.region,
                COUNT(DISTINCT o.order_id) AS total_orders,
                SUM(CASE WHEN o.is_on_time = 1 THEN 1 ELSE 0 END) AS on_time_orders,
                ROUND(AVG(o.actual_transit_days), 2) AS avg_transit_days,
                ROUND(AVG(o.carrier_delay_days), 2) AS avg_delay_days,
                ROUND((SUM(CASE WHEN o.is_on_time = 1 THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(DISTINCT o.order_id), 0)) * 100, 2) AS on_time_delivery_rate_pct
            FROM stg_orders o
            JOIN stg_order_items i ON o.order_id = i.order_id
            JOIN dim_warehouses w ON i.warehouse_id = w.warehouse_id
            WHERE o.order_status = 'delivered'
            GROUP BY i.warehouse_id, w.warehouse_name, w.region;
        """)

        # 3. fct_stockout_risks (Advanced Window SQL & Reorder Engine)
        self.con.execute("""
            CREATE OR REPLACE TABLE fct_stockout_risks AS
            WITH latest_snapshots AS (
                SELECT
                    inv.*,
                    ROW_NUMBER() OVER (PARTITION BY inv.warehouse_id, inv.product_id ORDER BY inv.snapshot_date DESC) as rn
                FROM stg_inventory inv
            ),
            current_positions AS (
                SELECT
                    s.snapshot_date,
                    s.warehouse_id,
                    w.warehouse_name,
                    w.region,
                    s.product_id,
                    p.product_name,
                    p.category,
                    p.unit_price,
                    s.stock_on_hand,
                    s.safety_stock_level,
                    s.reorder_point,
                    s.lead_time_days,
                    s.is_below_safety_stock,
                    s.is_stockout
                FROM latest_snapshots s
                JOIN raw_products p ON s.product_id = p.product_id
                JOIN dim_warehouses w ON s.warehouse_id = w.warehouse_id
                WHERE s.rn = 1
            ),
            demand_history AS (
                SELECT
                    warehouse_id,
                    product_id,
                    ROUND(AVG(daily_demand), 2) AS avg_daily_demand_30d,
                    MAX(daily_demand) AS peak_daily_demand
                FROM stg_inventory
                GROUP BY warehouse_id, product_id
            )
            SELECT
                c.warehouse_id,
                c.warehouse_name,
                c.region,
                c.product_id,
                c.product_name,
                c.category,
                c.unit_price,
                c.stock_on_hand,
                c.safety_stock_level,
                c.reorder_point,
                c.lead_time_days,
                d.avg_daily_demand_30d,
                d.peak_daily_demand,
                -- Days of Supply (DoS)
                ROUND(CASE 
                    WHEN d.avg_daily_demand_30d > 0 THEN c.stock_on_hand / d.avg_daily_demand_30d 
                    ELSE 999.0 
                END, 1) AS days_of_supply,
                -- Recommended Reorder Quantity formula:
                -- Reorder Qty = (Lead Time * Avg Daily Demand) + Safety Stock - Stock on Hand
                GREATEST(0, CAST(ROUND((c.lead_time_days * d.avg_daily_demand_30d) + c.safety_stock_level - c.stock_on_hand) AS INTEGER)) AS recommended_reorder_qty,
                -- Capital Value at Risk ($)
                ROUND(GREATEST(0, (c.lead_time_days * d.avg_daily_demand_30d) + c.safety_stock_level - c.stock_on_hand) * c.unit_price, 2) AS stockout_capital_at_risk,
                -- Stockout Risk Score (0 - 100)
                ROUND(
                    CASE
                        WHEN c.stock_on_hand = 0 THEN 100.0
                        WHEN (c.stock_on_hand / NULLIF(d.avg_daily_demand_30d, 0)) <= c.lead_time_days * 0.5 THEN 85.0
                        WHEN c.stock_on_hand <= c.safety_stock_level THEN 65.0
                        WHEN c.stock_on_hand <= c.reorder_point THEN 40.0
                        ELSE 10.0
                    END, 1
                ) AS risk_score,
                CASE
                    WHEN c.stock_on_hand = 0 THEN 'CRITICAL_STOCKOUT'
                    WHEN (c.stock_on_hand / NULLIF(d.avg_daily_demand_30d, 0)) <= c.lead_time_days * 0.5 THEN 'HIGH_IMMINENT_STOCKOUT'
                    WHEN c.stock_on_hand <= c.safety_stock_level THEN 'MODERATE_SAFETY_BREACH'
                    WHEN c.stock_on_hand <= c.reorder_point THEN 'LOW_REORDER_TRIGGERED'
                    ELSE 'HEALTHY'
                END AS stockout_alert_status
            FROM current_positions c
            JOIN demand_history d ON c.warehouse_id = d.warehouse_id AND c.product_id = d.product_id;
        """)

    def query(self, sql: str) -> Tuple[pd.DataFrame, float]:
        """Executes an arbitrary SQL query and returns (df, execution_time_ms)."""
        t0 = time.perf_counter()
        df = self.con.execute(sql).df()
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        return df, round(elapsed_ms, 3)

    def get_stockout_summary(self) -> Dict[str, Any]:
        """Computes summary statistics on current stockout risks across warehouses."""
        df, _ = self.query("SELECT * FROM fct_stockout_risks;")
        total_skus = len(df)
        critical = len(df[df["stockout_alert_status"] == "CRITICAL_STOCKOUT"])
        high = len(df[df["stockout_alert_status"] == "HIGH_IMMINENT_STOCKOUT"])
        moderate = len(df[df["stockout_alert_status"] == "MODERATE_SAFETY_BREACH"])
        healthy = len(df[df["stockout_alert_status"] == "HEALTHY"])
        total_risk_val = df["stockout_capital_at_risk"].sum()

        return {
            "total_monitored_skus": total_skus,
            "critical_stockouts": critical,
            "high_imminent_stockouts": high,
            "moderate_safety_breaches": moderate,
            "healthy_skus": healthy,
            "total_capital_at_risk_usd": round(float(total_risk_val), 2),
            "critical_percentage": round((critical / total_skus) * 100, 1) if total_skus > 0 else 0.0
        }

    def close(self):
        """Closes the DuckDB connection."""
        try:
            self.con.close()
        except Exception:
            pass
