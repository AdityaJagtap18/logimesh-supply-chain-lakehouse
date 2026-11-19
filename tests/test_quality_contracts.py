"""Tests for Great Expectations data quality contracts."""

import pytest
import pandas as pd
from src.core.generator import SupplyChainDataGenerator
from src.core.quality_gate import DataQualityGate


def test_quality_gate_passes_clean_data(tmp_path):
    gen = SupplyChainDataGenerator(seed=42)
    orders, items = gen.generate_orders_and_items(order_count=100)
    inv = gen.generate_inventory_snapshots(orders, items, days_span=5)

    gate = DataQualityGate(reports_dir=str(tmp_path))
    report = gate.run_full_suite(orders, items, inv)

    assert report["overall_status"] == "PASSED"
    assert report["pass_rate_pct"] == 100.0
    assert report["failed_assertions"] == 0
    assert report["total_assertions"] == 11


def test_quality_gate_catches_invalid_order_status(tmp_path):
    gen = SupplyChainDataGenerator(seed=42)
    orders, items = gen.generate_orders_and_items(order_count=50)
    # Inject poisoned status
    orders.loc[0, "order_status"] = "corrupted_status_xyz"

    gate = DataQualityGate(reports_dir=str(tmp_path))
    checks = gate.validate_orders(orders)
    status_check = next(c for c in checks if c.check_name == "orders_status_valid_domain")
    assert not status_check.success


def test_quality_gate_catches_negative_price(tmp_path):
    gen = SupplyChainDataGenerator(seed=42)
    orders, items = gen.generate_orders_and_items(order_count=50)
    # Inject negative price
    items.loc[0, "price"] = -19.99

    gate = DataQualityGate(reports_dir=str(tmp_path))
    checks = gate.validate_order_items(items)
    price_check = next(c for c in checks if c.check_name == "items_price_strictly_positive")
    assert not price_check.success
