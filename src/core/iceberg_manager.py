"""Apache Iceberg table manager for LogiMesh Lakehouse.

Implements Iceberg table creation, snapshot tracking, manifest logging,
partition specification, schema evolution, and Time Travel queries.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
import os
import time
from typing import Any, Dict, List, Optional
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


@dataclass
class IcebergSnapshot:
    snapshot_id: int
    parent_snapshot_id: Optional[int]
    timestamp_ms: int
    manifest_list: str
    summary: Dict[str, str] = field(default_factory=dict)


class IcebergTableManager:
    """Manages Iceberg table catalogs, ACID snapshots, and metadata evolution."""

    def __init__(self, warehouse_path: str = "data/iceberg"):
        self.warehouse_path = warehouse_path
        os.makedirs(self.warehouse_path, exist_ok=True)

    def _table_dir(self, table_name: str) -> str:
        return os.path.join(self.warehouse_path, table_name)

    def _metadata_dir(self, table_name: str) -> str:
        return os.path.join(self._table_dir(table_name), "metadata")

    def _data_dir(self, table_name: str) -> str:
        return os.path.join(self._table_dir(table_name), "data")

    def create_or_append_table(
        self,
        table_name: str,
        df: pd.DataFrame,
        partition_col: Optional[str] = None,
        operation: str = "append"
    ) -> Dict[str, Any]:
        """Writes data to the Iceberg table and registers an ACID snapshot."""
        table_dir = self._table_dir(table_name)
        meta_dir = self._metadata_dir(table_name)
        data_dir = self._data_dir(table_name)
        os.makedirs(meta_dir, exist_ok=True)
        os.makedirs(data_dir, exist_ok=True)

        snapshot_id = int(time.time() * 1000)
        timestamp_ms = int(time.time() * 1000)
        timestamp_iso = datetime.now(timezone.utc).isoformat()

        # Write Parquet data file
        file_name = f"{table_name}-{snapshot_id}.parquet"
        file_path = os.path.join(data_dir, file_name)

        # Convert timestamps for compatibility
        clean_df = df.copy()
        for col in clean_df.select_dtypes(include=["datetime64[ns]", "datetime64[ns, UTC]"]).columns:
            clean_df[col] = clean_df[col].astype(str)

        clean_df.to_parquet(file_path, index=False)
        added_records = len(df)
        file_size_bytes = os.path.getsize(file_path)

        # Load previous metadata if exists
        meta_file = os.path.join(meta_dir, f"{table_name}_metadata.json")
        parent_id = None
        history = []
        current_schema = {col: str(clean_df[col].dtype) for col in clean_df.columns}

        if os.path.exists(meta_file):
            try:
                with open(meta_file, "r") as f:
                    old_meta = json.load(f)
                    parent_id = old_meta.get("current_snapshot_id")
                    history = old_meta.get("snapshots", [])
            except Exception:
                pass

        snapshot_entry = {
            "snapshot_id": snapshot_id,
            "parent_snapshot_id": parent_id,
            "timestamp_ms": timestamp_ms,
            "timestamp_iso": timestamp_iso,
            "operation": operation,
            "manifest_file": f"manifest-{snapshot_id}.avro",
            "data_file": file_name,
            "summary": {
                "operation": operation,
                "added_records": added_records,
                "added_data_files": 1,
                "added_files_size_bytes": file_size_bytes,
                "partition_col": partition_col or "unpartitioned",
            }
        }
        history.append(snapshot_entry)

        metadata = {
            "format_version": 2,
            "table_name": table_name,
            "table_uuid": f"iceberg-tbl-{table_name}",
            "location": table_dir,
            "last_updated_ms": timestamp_ms,
            "last_updated_iso": timestamp_iso,
            "current_snapshot_id": snapshot_id,
            "partition_spec": [{"column": partition_col, "transform": "identity"}] if partition_col else [],
            "schema": current_schema,
            "snapshots": history,
            "properties": {
                "write.format.default": "parquet",
                "write.metadata.compression-codec": "gzip"
            }
        }

        with open(meta_file, "w") as f:
            json.dump(metadata, f, indent=2)

        return {
            "table_name": table_name,
            "snapshot_id": snapshot_id,
            "parent_snapshot_id": parent_id,
            "records_written": added_records,
            "file_path": file_path,
            "metadata_file": meta_file,
        }

    def evolve_schema_add_column(
        self,
        table_name: str,
        new_column: str,
        default_val: Any,
        data_type: str = "string"
    ) -> Dict[str, Any]:
        """Evolves table schema by appending a new column attribute without rewriting data."""
        meta_dir = self._metadata_dir(table_name)
        meta_file = os.path.join(meta_dir, f"{table_name}_metadata.json")

        if not os.path.exists(meta_file):
            raise FileNotFoundError(f"Table metadata for {table_name} does not exist.")

        with open(meta_file, "r") as f:
            meta = json.load(f)

        meta["schema"][new_column] = data_type
        meta["last_updated_ms"] = int(time.time() * 1000)
        meta["last_updated_iso"] = datetime.now(timezone.utc).isoformat()

        # Log schema evolution event in snapshot log
        schema_snapshot_id = int(time.time() * 1000)
        meta["snapshots"].append({
            "snapshot_id": schema_snapshot_id,
            "parent_snapshot_id": meta.get("current_snapshot_id"),
            "timestamp_ms": meta["last_updated_ms"],
            "timestamp_iso": meta["last_updated_iso"],
            "operation": "schema_evolution",
            "manifest_file": f"manifest-{schema_snapshot_id}.avro",
            "data_file": "none (metadata only)",
            "summary": {
                "operation": "schema_evolution",
                "added_column": new_column,
                "data_type": data_type,
                "default_value": str(default_val),
            }
        })
        meta["current_snapshot_id"] = schema_snapshot_id

        with open(meta_file, "w") as f:
            json.dump(meta, f, indent=2)

        return {
            "status": "evolved",
            "table_name": table_name,
            "new_column": new_column,
            "schema": meta["schema"]
        }

    def get_table_metadata(self, table_name: str) -> Optional[Dict[str, Any]]:
        """Returns the current Iceberg table metadata dictionary."""
        meta_file = os.path.join(self._metadata_dir(table_name), f"{table_name}_metadata.json")
        if not os.path.exists(meta_file):
            return None
        with open(meta_file, "r") as f:
            return json.load(f)

    def list_snapshots(self, table_name: str) -> List[Dict[str, Any]]:
        """Returns the list of historical snapshots for Time Travel."""
        meta = self.get_table_metadata(table_name)
        if not meta:
            return []
        return meta.get("snapshots", [])

    def load_as_snapshot(self, table_name: str, snapshot_id: int) -> pd.DataFrame:
        """Time Travel: loads the exact data version associated with a snapshot_id."""
        meta = self.get_table_metadata(table_name)
        if not meta:
            raise FileNotFoundError(f"Table {table_name} not found.")

        target_snap = None
        for s in meta.get("snapshots", []):
            if s["snapshot_id"] == snapshot_id:
                target_snap = s
                break

        if not target_snap:
            raise ValueError(f"Snapshot ID {snapshot_id} not found in history for {table_name}.")

        data_file = target_snap.get("data_file")
        if not data_file or data_file.startswith("none"):
            # If metadata-only snapshot, fall back to parent data file
            parent_id = target_snap.get("parent_snapshot_id")
            if parent_id:
                return self.load_as_snapshot(table_name, parent_id)
            raise ValueError(f"Snapshot {snapshot_id} has no associated data files.")

        file_path = os.path.join(self._data_dir(table_name), data_file)
        return pd.read_parquet(file_path)

    def load_current_table(self, table_name: str) -> pd.DataFrame:
        """Loads the current latest version of the Iceberg table."""
        meta = self.get_table_metadata(table_name)
        if not meta or not meta.get("current_snapshot_id"):
            raise FileNotFoundError(f"No active snapshots found for table {table_name}.")
        return self.load_as_snapshot(table_name, meta["current_snapshot_id"])
