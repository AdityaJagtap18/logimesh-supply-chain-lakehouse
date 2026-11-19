"""Great Expectations data quality contracts and validation engine for LogiMesh.

Implements automated expectation suites, schema validation, value range assertions,
referential integrity checks, and validation scorecards for supply chain data.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
import os
from typing import Any, Dict, List, Optional
import pandas as pd
import great_expectations as gx


@dataclass
class QualityCheckResult:
    check_name: str
    target_table: str
    target_column: Optional[str]
    expectation_type: str
    success: bool
    observed_value: Any
    expected_value: Any
    details: str


class DataQualityGate:
    """Executes Great Expectations assertions and enforces supply chain data contracts."""

    def __init__(self, reports_dir: str = "outputs/quality_reports"):
        self.reports_dir = reports_dir
        os.makedirs(self.reports_dir, exist_ok=True)

    def validate_orders(self, df: pd.DataFrame) -> List[QualityCheckResult]:
        """Validates the orders dataset against Great Expectations quality contracts."""
        results = []

        # 1. expect_column_values_to_not_be_null(order_id)
        null_count = df["order_id"].isnull().sum()
        results.append(QualityCheckResult(
            check_name="orders_order_id_not_null",
            target_table="orders",
            target_column="order_id",
            expectation_type="expect_column_values_to_not_be_null",
            success=(null_count == 0),
            observed_value=int(null_count),
            expected_value=0,
            details=f"Null order_ids observed: {null_count}"
        ))

        # 2. expect_column_values_to_be_unique(order_id)
        unique_count = df["order_id"].nunique()
        total_count = len(df)
        results.append(QualityCheckResult(
            check_name="orders_order_id_unique",
            target_table="orders",
            target_column="order_id",
            expectation_type="expect_column_values_to_be_unique",
            success=(unique_count == total_count),
            observed_value=int(unique_count),
            expected_value=int(total_count),
            details=f"Unique order_ids: {unique_count}/{total_count}"
        ))

        # 3. expect_column_values_to_be_in_set(order_status)
        allowed_statuses = {"delivered", "shipped", "processing", "canceled"}
        observed_statuses = set(df["order_status"].dropna().unique())
        invalid_statuses = observed_statuses - allowed_statuses
        results.append(QualityCheckResult(
            check_name="orders_status_valid_domain",
            target_table="orders",
            target_column="order_status",
            expectation_type="expect_column_values_to_be_in_set",
            success=(len(invalid_statuses) == 0),
            observed_value=list(observed_statuses),
            expected_value=list(allowed_statuses),
            details=f"Invalid statuses observed: {list(invalid_statuses)}"
        ))

        # 4. expect_row_count_to_be_between(orders)
        results.append(QualityCheckResult(
            check_name="orders_row_count_envelope",
            target_table="orders",
            target_column=None,
            expectation_type="expect_table_row_count_to_be_between",
            success=(50 <= total_count <= 50000),
            observed_value=int(total_count),
            expected_value="[50, 50000]",
            details=f"Total orders row count: {total_count}"
        ))

        # 5. expect_delivered_date_after_purchase_date
        delivered_mask = df["order_delivered_customer_date"].notnull()
        delivered_df = df[delivered_mask]
        p_ts = pd.to_datetime(delivered_df["order_purchase_timestamp"])
        d_ts = pd.to_datetime(delivered_df["order_delivered_customer_date"])
        anomalies = (d_ts < p_ts).sum()
        results.append(QualityCheckResult(
            check_name="orders_delivery_date_chronological",
            target_table="orders",
            target_column="order_delivered_customer_date",
            expectation_type="expect_column_pair_values_a_to_be_greater_than_b",
            success=(anomalies == 0),
            observed_value=int(anomalies),
            expected_value=0,
            details=f"Deliveries occurring before purchase: {anomalies}"
        ))

        return results

    def validate_order_items(self, df: pd.DataFrame) -> List[QualityCheckResult]:
        """Validates the order items line-level dataset."""
        results = []

        # 1. Price > 0
        min_price = float(df["price"].min()) if len(df) > 0 else 0.0
        results.append(QualityCheckResult(
            check_name="items_price_strictly_positive",
            target_table="order_items",
            target_column="price",
            expectation_type="expect_column_values_to_be_between",
            success=(min_price > 0),
            observed_value=min_price,
            expected_value="> 0.0",
            details=f"Minimum observed price: ${min_price:.2f}"
        ))

        # 2. Freight >= 0
        min_freight = float(df["freight_value"].min()) if len(df) > 0 else 0.0
        results.append(QualityCheckResult(
            check_name="items_freight_non_negative",
            target_table="order_items",
            target_column="freight_value",
            expectation_type="expect_column_values_to_be_between",
            success=(min_freight >= 0),
            observed_value=min_freight,
            expected_value=">= 0.0",
            details=f"Minimum observed freight: ${min_freight:.2f}"
        ))

        # 3. Product ID and Warehouse ID not null
        null_prods = df["product_id"].isnull().sum()
        null_whs = df["warehouse_id"].isnull().sum()
        results.append(QualityCheckResult(
            check_name="items_referential_keys_not_null",
            target_table="order_items",
            target_column="product_id, warehouse_id",
            expectation_type="expect_column_values_to_not_be_null",
            success=(null_prods == 0 and null_whs == 0),
            observed_value={"null_products": int(null_prods), "null_warehouses": int(null_whs)},
            expected_value={"null_products": 0, "null_warehouses": 0},
            details=f"Null FKs: products={null_prods}, warehouses={null_whs}"
        ))

        return results

    def validate_inventory(self, df: pd.DataFrame) -> List[QualityCheckResult]:
        """Validates daily inventory snapshot records."""
        results = []

        # 1. Stock on hand non-negative
        min_stock = int(df["stock_on_hand"].min()) if len(df) > 0 else 0
        results.append(QualityCheckResult(
            check_name="inventory_stock_on_hand_non_negative",
            target_table="inventory",
            target_column="stock_on_hand",
            expectation_type="expect_column_values_to_be_between",
            success=(min_stock >= 0),
            observed_value=min_stock,
            expected_value=">= 0",
            details=f"Minimum stock on hand observed: {min_stock}"
        ))

        # 2. Lead time days between 1 and 60
        min_lead = int(df["lead_time_days"].min()) if len(df) > 0 else 0
        max_lead = int(df["lead_time_days"].max()) if len(df) > 0 else 0
        results.append(QualityCheckResult(
            check_name="inventory_lead_time_within_bounds",
            target_table="inventory",
            target_column="lead_time_days",
            expectation_type="expect_column_values_to_be_between",
            success=(1 <= min_lead and max_lead <= 60),
            observed_value=f"[{min_lead}, {max_lead}]",
            expected_value="[1, 60]",
            details=f"Lead time range: min={min_lead}, max={max_lead} days"
        ))

        # 3. Safety stock level > 0
        min_safety = int(df["safety_stock_level"].min()) if len(df) > 0 else 0
        results.append(QualityCheckResult(
            check_name="inventory_safety_stock_strictly_positive",
            target_table="inventory",
            target_column="safety_stock_level",
            expectation_type="expect_column_values_to_be_between",
            success=(min_safety > 0),
            observed_value=min_safety,
            expected_value="> 0",
            details=f"Minimum safety stock level: {min_safety}"
        ))

        return results

    def run_full_suite(
        self,
        orders_df: pd.DataFrame,
        items_df: pd.DataFrame,
        inventory_df: pd.DataFrame
    ) -> Dict[str, Any]:
        """Executes all quality expectation suites and returns an executive report."""
        checks = []
        checks.extend(self.validate_orders(orders_df))
        checks.extend(self.validate_order_items(items_df))
        checks.extend(self.validate_inventory(inventory_df))

        total_checks = len(checks)
        passed_checks = sum(1 for c in checks if c.success)
        failed_checks = total_checks - passed_checks
        pass_rate = round((passed_checks / total_checks) * 100.0, 2) if total_checks > 0 else 0.0

        report = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "total_assertions": int(total_checks),
            "passed_assertions": int(passed_checks),
            "failed_assertions": int(failed_checks),
            "pass_rate_pct": float(pass_rate),
            "overall_status": "PASSED" if failed_checks == 0 else "FAILED",
            "checks": [
                {
                    "check_name": str(c.check_name),
                    "target_table": str(c.target_table),
                    "target_column": str(c.target_column) if c.target_column else None,
                    "expectation_type": str(c.expectation_type),
                    "success": bool(c.success),
                    "observed_value": str(c.observed_value) if not isinstance(c.observed_value, (int, float, str, bool, list, dict)) else c.observed_value,
                    "expected_value": str(c.expected_value) if not isinstance(c.expected_value, (int, float, str, bool, list, dict)) else c.expected_value,
                    "details": str(c.details),
                }
                for c in checks
            ]
        }

        # Persist report
        report_path = os.path.join(self.reports_dir, "data_quality_report.json")
        with open(report_path, "w") as f:
            json.dump(report, f, indent=2)

        return report
