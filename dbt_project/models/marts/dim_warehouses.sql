{{ config(materialized='table') }}

SELECT
    warehouse_id,
    warehouse_name,
    region,
    capacity_sqft,
    operating_cost_per_day
FROM raw_warehouses
