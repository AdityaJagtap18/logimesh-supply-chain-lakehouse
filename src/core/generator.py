"""Synthetic logistics and supply chain data generator for LogiMesh.

Modeled on Olist e-commerce logistics and TPC-DS supply chain schemas.
Generates realistic multi-tier purchase orders, shipment transit times,
and daily warehouse inventory snapshots.
"""

from datetime import datetime, timedelta
import random
from typing import Dict, List, Tuple
import pandas as pd
import numpy as np


class SupplyChainDataGenerator:
    """Generates synthetic supply chain, inventory, and logistics datasets."""

    WAREHOUSE_SPECS = [
        {"id": "WH-NORTH-01", "name": "Chicago Central Distribution Hub", "region": "Midwest", "sqft": 450000, "cost_day": 8500.0},
        {"id": "WH-SOUTH-02", "name": "Dallas Logistics & Fulfillment Park", "region": "South", "sqft": 600000, "cost_day": 9200.0},
        {"id": "WH-EAST-03", "name": "Newark Port Gateway Fulfillment", "region": "Northeast", "sqft": 520000, "cost_day": 11000.0},
        {"id": "WH-WEST-04", "name": "Inland Empire Logistics Center", "region": "West", "sqft": 750000, "cost_day": 13500.0},
        {"id": "WH-CENTRAL-05", "name": "Kansas City Cross-Dock Hub", "region": "Central", "sqft": 380000, "cost_day": 7200.0},
    ]

    CATEGORIES = [
        ("Electronics", 120.0, 950.0, 14, 25),
        ("Medical Supplies", 45.0, 480.0, 7, 50),
        ("Apparel & Footwear", 25.0, 180.0, 21, 80),
        ("Industrial Tools", 85.0, 650.0, 30, 15),
        ("Home & Kitchen", 30.0, 220.0, 18, 60),
    ]

    def __init__(self, seed: int = 42):
        self.seed = seed
        random.seed(seed)
        np.random.seed(seed)
        self.products = self._generate_products(count=50)

    def _generate_products(self, count: int = 50) -> List[Dict]:
        """Generate product master catalog with categories, baseline prices, and lead times."""
        products = []
        for i in range(1, count + 1):
            cat, min_p, max_p, base_lead, base_safety = random.choice(self.CATEGORIES)
            price = round(random.uniform(min_p, max_p), 2)
            lead_time = max(3, int(np.random.normal(base_lead, 4)))
            safety_stock = max(10, int(np.random.normal(base_safety, 12)))
            reorder_point = int(safety_stock + (lead_time * random.uniform(2.0, 6.0)))

            products.append({
                "product_id": f"SKU-{i:04d}",
                "product_name": f"{cat} Model {i:03d}",
                "category": cat,
                "unit_price": price,
                "lead_time_days": lead_time,
                "safety_stock_level": safety_stock,
                "reorder_point": reorder_point,
            })
        return products

    def generate_warehouses_df(self) -> pd.DataFrame:
        """Returns the warehouse master DataFrame."""
        return pd.DataFrame(self.WAREHOUSE_SPECS).rename(columns={
            "id": "warehouse_id",
            "name": "warehouse_name",
            "sqft": "capacity_sqft",
            "cost_day": "operating_cost_per_day"
        })

    def generate_products_df(self) -> pd.DataFrame:
        """Returns the product master DataFrame."""
        return pd.DataFrame(self.products)

    def generate_orders_and_items(
        self,
        order_count: int = 1500,
        start_date: datetime = datetime(2026, 1, 1),
        days_span: int = 90
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Generate realistic orders and order line items with shipping lead times."""
        orders = []
        order_items = []
        statuses = ["delivered", "delivered", "delivered", "shipped", "processing", "canceled"]
        status_weights = [0.82, 0.05, 0.04, 0.05, 0.03, 0.01]

        for i in range(1, order_count + 1):
            order_id = f"ORD-{i:06d}"
            customer_id = f"CUST-{random.randint(1000, 9999)}"
            order_days_offset = random.uniform(0, days_span)
            purchase_ts = start_date + timedelta(days=order_days_offset)
            status = random.choices(statuses, weights=status_weights)[0]

            # Delivery timeline modeling
            carrier_delay_days = random.choices([0, 1, 2, 4, 8], weights=[0.75, 0.12, 0.08, 0.03, 0.02])[0]
            est_transit_days = random.randint(3, 7)
            estimated_delivery = purchase_ts + timedelta(days=est_transit_days)

            if status == "delivered":
                actual_transit_days = est_transit_days + carrier_delay_days
                delivered_ts = purchase_ts + timedelta(days=actual_transit_days)
            elif status == "shipped":
                delivered_ts = None
            else:
                delivered_ts = None
                estimated_delivery = None if status == "canceled" else estimated_delivery

            orders.append({
                "order_id": order_id,
                "customer_id": customer_id,
                "order_status": status,
                "order_purchase_timestamp": purchase_ts,
                "order_estimated_delivery_date": estimated_delivery,
                "order_delivered_customer_date": delivered_ts,
            })

            # Line items (1 to 4 items per order)
            num_items = random.choices([1, 2, 3, 4], weights=[0.60, 0.25, 0.10, 0.05])[0]
            for item_idx in range(1, num_items + 1):
                prod = random.choice(self.products)
                wh = random.choice(self.WAREHOUSE_SPECS)
                qty = random.randint(1, 3)
                price = prod["unit_price"]
                freight = round(random.uniform(5.5, 28.0) * qty, 2)

                order_items.append({
                    "order_id": order_id,
                    "item_id": item_idx,
                    "product_id": prod["product_id"],
                    "warehouse_id": wh["id"],
                    "quantity": qty,
                    "price": price,
                    "freight_value": freight,
                })

        orders_df = pd.DataFrame(orders)
        items_df = pd.DataFrame(order_items)
        return orders_df, items_df

    def generate_inventory_snapshots(
        self,
        orders_df: pd.DataFrame,
        items_df: pd.DataFrame,
        start_date: datetime = datetime(2026, 1, 1),
        days_span: int = 90
    ) -> pd.DataFrame:
        """Simulate daily inventory levels across warehouses with demand and replenishment."""
        snapshots = []
        wh_ids = [wh["id"] for wh in self.WAREHOUSE_SPECS]

        # Initial inventory per warehouse & product
        curr_stock = {}
        for wh_id in wh_ids:
            for prod in self.products:
                initial_qty = int(prod["safety_stock_level"] * random.uniform(1.8, 3.5))
                curr_stock[(wh_id, prod["product_id"])] = initial_qty

        # Map daily demand from items
        items_with_ts = items_df.merge(
            orders_df[["order_id", "order_purchase_timestamp", "order_status"]],
            on="order_id"
        )
        items_with_ts["purchase_date"] = items_with_ts["order_purchase_timestamp"].dt.date

        # Group demand by (date, warehouse_id, product_id)
        valid_demand = items_with_ts[items_with_ts["order_status"] != "canceled"]
        demand_map = valid_demand.groupby(["purchase_date", "warehouse_id", "product_id"])["quantity"].sum().to_dict()

        for day in range(days_span):
            curr_date = (start_date + timedelta(days=day)).date()

            for wh_id in wh_ids:
                for prod in self.products:
                    pid = prod["product_id"]
                    key = (wh_id, pid)
                    demand_today = demand_map.get((curr_date, wh_id, pid), 0)

                    # Deduct demand
                    stock_before = curr_stock[key]
                    stock_after = max(0, stock_before - demand_today)

                    # Replenishment logic with occasional supplier disruptions
                    reorder_pt = prod["reorder_point"]
                    # 10% of items suffer supplier delays (no replenishment for prolonged window)
                    is_disrupted_sku = (hash(pid) % 10 == 0)

                    if stock_after <= reorder_pt:
                        if is_disrupted_sku:
                            # Disrupted supply chain: replenishment fails or is delayed
                            pass
                        elif random.random() < 0.25:
                            replenishment_qty = int(prod["safety_stock_level"] * random.uniform(2.0, 4.0))
                            stock_after += replenishment_qty

                    curr_stock[key] = stock_after

                    snapshots.append({
                        "snapshot_date": pd.to_datetime(curr_date),
                        "warehouse_id": wh_id,
                        "product_id": pid,
                        "stock_on_hand": stock_after,
                        "daily_demand": demand_today,
                        "safety_stock_level": prod["safety_stock_level"],
                        "reorder_point": prod["reorder_point"],
                        "lead_time_days": prod["lead_time_days"],
                    })

        return pd.DataFrame(snapshots)

    def generate_full_dataset(
        self,
        raw_output_dir: str = "data/raw"
    ) -> Dict[str, pd.DataFrame]:
        """Generates all 4 tables and persists them as Parquet files in raw_output_dir."""
        import os
        os.makedirs(raw_output_dir, exist_ok=True)

        wh_df = self.generate_warehouses_df()
        prod_df = self.generate_products_df()
        orders_df, items_df = self.generate_orders_and_items()
        inv_df = self.generate_inventory_snapshots(orders_df, items_df)

        wh_df.to_parquet(os.path.join(raw_output_dir, "warehouses.parquet"), index=False)
        prod_df.to_parquet(os.path.join(raw_output_dir, "products.parquet"), index=False)
        orders_df.to_parquet(os.path.join(raw_output_dir, "orders.parquet"), index=False)
        items_df.to_parquet(os.path.join(raw_output_dir, "order_items.parquet"), index=False)
        inv_df.to_parquet(os.path.join(raw_output_dir, "inventory.parquet"), index=False)

        return {
            "warehouses": wh_df,
            "products": prod_df,
            "orders": orders_df,
            "order_items": items_df,
            "inventory": inv_df,
        }
