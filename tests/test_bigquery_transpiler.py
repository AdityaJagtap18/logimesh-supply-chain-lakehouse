"""Tests for BigQuery external table DDL generator and SQL transpiler."""

import pytest
from src.warehouse.bigquery_manager import BigQueryWarehouseManager


def test_bigquery_ddl_generation():
    bq = BigQueryWarehouseManager(
        project_id="test-proj",
        dataset_id="test_ds",
    )
    ddls = bq.generate_all_ddls()
    assert "orders" in ddls
    assert "inventory" in ddls

    orders_ddl = ddls["orders"]
    assert "CREATE OR REPLACE EXTERNAL TABLE" in orders_ddl
    assert "format = 'ICEBERG'" in orders_ddl
    assert "PARTITION BY DATE(order_purchase_timestamp)" in orders_ddl


def test_bigquery_sql_transpilation():
    bq = BigQueryWarehouseManager()
    duckdb_sql = "SELECT DATE_DIFF('day', order_date, delivery_date) AS days, val::FLOAT AS num FROM tbl;"
    transpiled = bq.transpile_duckdb_to_bigquery(duckdb_sql)
    assert "DATE_DIFF(DATE(delivery_date), DATE(order_date), DAY)" in transpiled
    assert "FLOAT64" in transpiled


def test_bigquery_cost_estimation():
    bq = BigQueryWarehouseManager()
    # 1 TB = 1024^4 bytes
    one_tb = 1024 ** 4
    cost = bq.estimate_query_cost(one_tb)
    assert cost["estimated_tb_scanned"] == 1.0
    assert cost["estimated_cost_usd"] == 6.25
