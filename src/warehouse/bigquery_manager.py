"""Google BigQuery enterprise lakehouse integration and SQL transpiler for LogiMesh.

Generates BigQuery DDLs for external Iceberg tables, defines partitioning and clustering,
transpiles DuckDB/ANSI SQL to BigQuery Standard SQL, and estimates query scan costs.
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional
import re


@dataclass
class BigQueryTableSpec:
    table_name: str
    partition_field: Optional[str]
    cluster_fields: List[str]
    description: str


class BigQueryWarehouseManager:
    """Manages Google BigQuery external table definitions and SQL dialect transpilation."""

    TABLE_SPECS = {
        "orders": BigQueryTableSpec(
            table_name="ext_orders",
            partition_field="order_purchase_timestamp",
            cluster_fields=["order_status", "customer_id"],
            description="External partitioned BigQuery table mapped to raw orders Iceberg storage."
        ),
        "order_items": BigQueryTableSpec(
            table_name="ext_order_items",
            partition_field=None,
            cluster_fields=["warehouse_id", "product_id"],
            description="Clustered BigQuery order items table for line-level freight and revenue."
        ),
        "inventory": BigQueryTableSpec(
            table_name="ext_inventory",
            partition_field="snapshot_date",
            cluster_fields=["warehouse_id", "product_id"],
            description="Daily warehouse inventory positions partitioned by snapshot date."
        ),
        "fct_stockout_risks": BigQueryTableSpec(
            table_name="fct_stockout_risks",
            partition_field=None,
            cluster_fields=["region", "stockout_alert_status"],
            description="Production analytical mart containing real-time stockout risk alerts."
        ),
    }

    def __init__(
        self,
        project_id: str = "logimesh-supply-chain-analytics",
        dataset_id: str = "logistics_lakehouse",
        location: str = "US"
    ):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.location = location

    def generate_external_iceberg_ddl(
        self,
        table_key: str,
        gcs_iceberg_uri: str = "gs://logimesh-lakehouse-bucket/iceberg"
    ) -> str:
        """Generates Google BigQuery standard SQL DDL for Iceberg external tables."""
        spec = self.TABLE_SPECS.get(table_key)
        if not spec:
            raise KeyError(f"Table spec '{table_key}' not defined.")

        partition_clause = ""
        if spec.partition_field:
            partition_clause = f"\nPARTITION BY DATE({spec.partition_field})"

        cluster_clause = ""
        if spec.cluster_fields:
            cluster_clause = f"\nCLUSTER BY {', '.join(spec.cluster_fields)}"

        table_id = f"`{self.project_id}.{self.dataset_id}.{spec.table_name}`"

        ddl = f"""-- BigQuery External Iceberg Table Definition
CREATE OR REPLACE EXTERNAL TABLE {table_id}{partition_clause}{cluster_clause}
OPTIONS (
    format = 'ICEBERG',
    uris = ['{gcs_iceberg_uri}/{table_key}/metadata/*.json'],
    description = '{spec.description}'
);"""
        return ddl

    def transpile_duckdb_to_bigquery(self, duckdb_sql: str) -> str:
        """Translates DuckDB SQL dialect nuances to Google BigQuery Standard SQL."""
        bq_sql = duckdb_sql

        # Replace DATE_DIFF('day', a, b) with DATE_DIFF(DATE(b), DATE(a), DAY)
        bq_sql = re.sub(
            r"DATE_DIFF\('day',\s*([a-zA-Z0-9_\(\)\s]+?),\s*([a-zA-Z0-9_\(\)\s]+?)\)",
            r"DATE_DIFF(DATE(\2), DATE(\1), DAY)",
            bq_sql,
            flags=re.IGNORECASE
        )

        # Replace ::FLOAT or ::DOUBLE with CAST(... AS FLOAT64)
        bq_sql = re.sub(r"::FLOAT", " AS FLOAT64)", bq_sql)
        bq_sql = re.sub(r"::DOUBLE", " AS FLOAT64)", bq_sql)

        # Replace ::INTEGER with CAST(... AS INT64)
        bq_sql = re.sub(r"::INTEGER", " AS INT64)", bq_sql)

        # Standardize strings
        bq_sql = bq_sql.replace("VARCHAR", "STRING")

        return bq_sql

    def estimate_query_cost(self, estimated_bytes_scanned: int) -> Dict[str, Any]:
        """Calculates BigQuery on-demand pricing ($6.25 per TB scanned)."""
        tb_scanned = estimated_bytes_scanned / (1024 ** 4)
        cost_usd = tb_scanned * 6.25
        return {
            "estimated_bytes_scanned": estimated_bytes_scanned,
            "estimated_tb_scanned": round(tb_scanned, 6),
            "estimated_cost_usd": round(cost_usd, 6),
            "pricing_tier": "BigQuery On-Demand ($6.25/TB)",
            "free_tier_eligible": estimated_bytes_scanned <= (1024 ** 4),  # 1 TB free per month
        }

    def generate_all_ddls(self) -> Dict[str, str]:
        """Generates DDL definitions for all lakehouse tables."""
        return {
            tbl: self.generate_external_iceberg_ddl(tbl)
            for tbl in self.TABLE_SPECS
        }
