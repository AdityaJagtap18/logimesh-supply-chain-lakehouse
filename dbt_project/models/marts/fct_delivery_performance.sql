{{ config(materialized='table') }}

SELECT
    i.warehouse_id,
    w.warehouse_name,
    w.region,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(CASE WHEN o.is_on_time = 1 THEN 1 ELSE 0 END) AS on_time_orders,
    ROUND(AVG(o.actual_transit_days), 2) AS avg_transit_days,
    ROUND(AVG(o.carrier_delay_days), 2) AS avg_delay_days,
    ROUND((SUM(CASE WHEN o.is_on_time = 1 THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(DISTINCT o.order_id), 0)) * 100, 2) AS on_time_delivery_rate_pct
FROM {{ ref('stg_orders') }} o
JOIN {{ ref('stg_order_items') }} i ON o.order_id = i.order_id
JOIN {{ ref('dim_warehouses') }} w ON i.warehouse_id = w.warehouse_id
WHERE o.order_status = 'delivered'
GROUP BY i.warehouse_id, w.warehouse_name, w.region
