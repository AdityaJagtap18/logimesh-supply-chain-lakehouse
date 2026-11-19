{{ config(materialized='table') }}

WITH latest_snapshots AS (
    SELECT
        inv.*,
        ROW_NUMBER() OVER (PARTITION BY inv.warehouse_id, inv.product_id ORDER BY inv.snapshot_date DESC) as rn
    FROM {{ ref('stg_inventory') }} inv
),
current_positions AS (
    SELECT
        s.snapshot_date,
        s.warehouse_id,
        w.warehouse_name,
        w.region,
        s.product_id,
        p.product_name,
        p.category,
        p.unit_price,
        s.stock_on_hand,
        s.safety_stock_level,
        s.reorder_point,
        s.lead_time_days,
        s.is_below_safety_stock,
        s.is_stockout
    FROM latest_snapshots s
    JOIN raw_products p ON s.product_id = p.product_id
    JOIN {{ ref('dim_warehouses') }} w ON s.warehouse_id = w.warehouse_id
    WHERE s.rn = 1
),
demand_history AS (
    SELECT
        warehouse_id,
        product_id,
        ROUND(AVG(daily_demand), 2) AS avg_daily_demand_30d,
        MAX(daily_demand) AS peak_daily_demand
    FROM {{ ref('stg_inventory') }}
    GROUP BY warehouse_id, product_id
)
SELECT
    c.warehouse_id,
    c.warehouse_name,
    c.region,
    c.product_id,
    c.product_name,
    c.category,
    c.unit_price,
    c.stock_on_hand,
    c.safety_stock_level,
    c.reorder_point,
    c.lead_time_days,
    d.avg_daily_demand_30d,
    d.peak_daily_demand,
    ROUND(CASE 
        WHEN d.avg_daily_demand_30d > 0 THEN c.stock_on_hand / d.avg_daily_demand_30d 
        ELSE 999.0 
    END, 1) AS days_of_supply,
    GREATEST(0, CAST(ROUND((c.lead_time_days * d.avg_daily_demand_30d) + c.safety_stock_level - c.stock_on_hand) AS INTEGER)) AS recommended_reorder_qty,
    ROUND(GREATEST(0, (c.lead_time_days * d.avg_daily_demand_30d) + c.safety_stock_level - c.stock_on_hand) * c.unit_price, 2) AS stockout_capital_at_risk,
    ROUND(
        CASE
            WHEN c.stock_on_hand = 0 THEN 100.0
            WHEN (c.stock_on_hand / NULLIF(d.avg_daily_demand_30d, 0)) <= c.lead_time_days * 0.5 THEN 85.0
            WHEN c.stock_on_hand <= c.safety_stock_level THEN 65.0
            WHEN c.stock_on_hand <= c.reorder_point THEN 40.0
            ELSE 10.0
        END, 1
    ) AS risk_score,
    CASE
        WHEN c.stock_on_hand = 0 THEN 'CRITICAL_STOCKOUT'
        WHEN (c.stock_on_hand / NULLIF(d.avg_daily_demand_30d, 0)) <= c.lead_time_days * 0.5 THEN 'HIGH_IMMINENT_STOCKOUT'
        WHEN c.stock_on_hand <= c.safety_stock_level THEN 'MODERATE_SAFETY_BREACH'
        WHEN c.stock_on_hand <= c.reorder_point THEN 'LOW_REORDER_TRIGGERED'
        ELSE 'HEALTHY'
    END AS stockout_alert_status
FROM current_positions c
JOIN demand_history d ON c.warehouse_id = d.warehouse_id AND c.product_id = d.product_id
