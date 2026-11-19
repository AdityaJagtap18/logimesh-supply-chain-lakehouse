"""Tests for DuckDB analytical OLAP engine and SQL transformations."""

import pytest
import pandas as pd
from src.core.generator import SupplyChainDataGenerator
from src.core.duckdb_engine import DuckDBAnalyticsEngine


@pytest.fixture
def staged_engine(tmp_path):
    raw_dir = tmp_path / "raw"
    db_file = tmp_path / "test_logimesh.duckdb"

    gen = SupplyChainDataGenerator(seed=42)
    gen.generate_full_dataset(raw_output_dir=str(raw_dir))

    engine = DuckDBAnalyticsEngine(db_path=str(db_file))
    engine.register_parquet_sources(raw_data_dir=str(raw_dir))
    engine.execute_staging_transformations()
    engine.execute_marts_transformations()
    yield engine
    engine.close()


def test_duckdb_staging_views(staged_engine):
    df_orders, _ = staged_engine.query("SELECT COUNT(*) as cnt FROM stg_orders;")
    assert df_orders["cnt"].iloc[0] > 0

    df_inv, _ = staged_engine.query("SELECT COUNT(*) as cnt FROM stg_inventory;")
    assert df_inv["cnt"].iloc[0] > 0


def test_duckdb_fct_delivery_performance(staged_engine):
    df_perf, _ = staged_engine.query("SELECT * FROM fct_delivery_performance;")
    assert len(df_perf) == 5
    assert "on_time_delivery_rate_pct" in df_perf.columns
    assert (df_perf["on_time_delivery_rate_pct"] >= 0).all()
    assert (df_perf["on_time_delivery_rate_pct"] <= 100).all()


def test_duckdb_fct_stockout_risks_calculation(staged_engine):
    df_risk, _ = staged_engine.query("SELECT * FROM fct_stockout_risks;")
    assert len(df_risk) == 5 * 50  # 250 SKU positions
    assert "days_of_supply" in df_risk.columns
    assert "recommended_reorder_qty" in df_risk.columns
    assert "risk_score" in df_risk.columns
    assert (df_risk["recommended_reorder_qty"] >= 0).all()

    summary = staged_engine.get_stockout_summary()
    assert summary["total_monitored_skus"] == 250
    assert summary["critical_stockouts"] >= 0
    assert summary["total_capital_at_risk_usd"] >= 0
