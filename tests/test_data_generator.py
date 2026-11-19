"""Tests for the synthetic supply chain data generator."""

import pytest
import pandas as pd
from src.core.generator import SupplyChainDataGenerator


def test_generator_warehouse_specs():
    gen = SupplyChainDataGenerator(seed=123)
    wh_df = gen.generate_warehouses_df()
    assert len(wh_df) == 5
    assert "warehouse_id" in wh_df.columns
    assert "capacity_sqft" in wh_df.columns
    assert wh_df["operating_cost_per_day"].min() > 0


def test_generator_product_catalog():
    gen = SupplyChainDataGenerator(seed=123)
    prod_df = gen.generate_products_df()
    assert len(prod_df) == 50
    assert "product_id" in prod_df.columns
    assert "lead_time_days" in prod_df.columns
    assert (prod_df["lead_time_days"] >= 3).all()
    assert (prod_df["unit_price"] > 0).all()


def test_generator_orders_and_items():
    gen = SupplyChainDataGenerator(seed=123)
    orders_df, items_df = gen.generate_orders_and_items(order_count=100)
    assert len(orders_df) == 100
    assert len(items_df) >= 100
    assert "order_id" in orders_df.columns
    assert "order_status" in orders_df.columns
    assert items_df["price"].min() > 0
    # Referential integrity
    valid_order_ids = set(orders_df["order_id"])
    assert set(items_df["order_id"]).issubset(valid_order_ids)


def test_generator_inventory_snapshots():
    gen = SupplyChainDataGenerator(seed=123)
    orders_df, items_df = gen.generate_orders_and_items(order_count=50)
    inv_df = gen.generate_inventory_snapshots(orders_df, items_df, days_span=10)
    assert len(inv_df) == 5 * 50 * 10  # 5 warehouses * 50 products * 10 days = 2500 rows
    assert (inv_df["stock_on_hand"] >= 0).all()
    assert (inv_df["lead_time_days"] > 0).all()
