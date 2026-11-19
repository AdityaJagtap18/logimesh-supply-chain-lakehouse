{{ config(materialized='view') }}

SELECT
    order_id,
    item_id,
    product_id,
    warehouse_id,
    quantity,
    price,
    freight_value,
    ROUND(quantity * price, 2) AS line_item_subtotal,
    ROUND((quantity * price) + freight_value, 2) AS line_item_total
FROM raw_order_items
