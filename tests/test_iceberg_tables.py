"""Tests for Apache Iceberg Lakehouse table management and Time Travel."""

import os
import shutil
import pytest
import pandas as pd
from src.core.iceberg_manager import IcebergTableManager


@pytest.fixture
def temp_iceberg_dir(tmp_path):
    d = tmp_path / "iceberg_warehouse"
    d.mkdir()
    return str(d)


def test_iceberg_table_creation_and_snapshot(temp_iceberg_dir):
    mgr = IcebergTableManager(warehouse_path=temp_iceberg_dir)
    df = pd.DataFrame({
        "order_id": ["ORD-001", "ORD-002"],
        "status": ["delivered", "shipped"],
        "amount": [120.5, 45.0]
    })
    res = mgr.create_or_append_table("test_orders", df, partition_col="status")
    assert res["records_written"] == 2
    assert res["snapshot_id"] > 0
    assert os.path.exists(res["metadata_file"])

    meta = mgr.get_table_metadata("test_orders")
    assert meta["format_version"] == 2
    assert meta["current_snapshot_id"] == res["snapshot_id"]
    assert len(meta["snapshots"]) == 1


def test_iceberg_time_travel(temp_iceberg_dir):
    mgr = IcebergTableManager(warehouse_path=temp_iceberg_dir)
    df_v1 = pd.DataFrame({"id": [1, 2], "val": [10, 20]})
    r1 = mgr.create_or_append_table("versioned_tbl", df_v1)
    snap1_id = r1["snapshot_id"]

    df_v2 = pd.DataFrame({"id": [1, 2, 3], "val": [10, 20, 30]})
    r2 = mgr.create_or_append_table("versioned_tbl", df_v2)
    snap2_id = r2["snapshot_id"]

    # Current table should have 3 rows
    curr_df = mgr.load_current_table("versioned_tbl")
    assert len(curr_df) == 3

    # Time Travel to snap1_id should have 2 rows
    hist_df = mgr.load_as_snapshot("versioned_tbl", snap1_id)
    assert len(hist_df) == 2
    assert list(hist_df["id"]) == [1, 2]


def test_iceberg_schema_evolution(temp_iceberg_dir):
    mgr = IcebergTableManager(warehouse_path=temp_iceberg_dir)
    df = pd.DataFrame({"id": [1], "name": ["Alpha"]})
    mgr.create_or_append_table("evolving_tbl", df)

    evolve_res = mgr.evolve_schema_add_column(
        "evolving_tbl",
        new_column="fulfillment_rating",
        default_val=5,
        data_type="int"
    )
    assert evolve_res["status"] == "evolved"
    assert "fulfillment_rating" in evolve_res["schema"]

    meta = mgr.get_table_metadata("evolving_tbl")
    assert len(meta["snapshots"]) == 2
