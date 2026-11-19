{{ config(materialized='view') }}

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
FROM raw_inventory
