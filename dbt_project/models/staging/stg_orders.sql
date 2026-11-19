{{ config(materialized='view') }}

SELECT
    order_id,
    customer_id,
    order_status,
    CAST(order_purchase_timestamp AS TIMESTAMP) AS purchase_timestamp,
    CAST(order_estimated_delivery_date AS TIMESTAMP) AS estimated_delivery_timestamp,
    CAST(order_delivered_customer_date AS TIMESTAMP) AS delivered_timestamp,
    CASE 
        WHEN order_delivered_customer_date IS NOT NULL 
        THEN DATE_DIFF('day', CAST(order_purchase_timestamp AS TIMESTAMP), CAST(order_delivered_customer_date AS TIMESTAMP))
        ELSE NULL
    END AS actual_transit_days,
    CASE
        WHEN order_delivered_customer_date IS NOT NULL AND order_estimated_delivery_date IS NOT NULL
        THEN GREATEST(0, DATE_DIFF('day', CAST(order_estimated_delivery_date AS TIMESTAMP), CAST(order_delivered_customer_date AS TIMESTAMP)))
        ELSE 0
    END AS carrier_delay_days,
    CASE
        WHEN order_delivered_customer_date IS NOT NULL AND order_estimated_delivery_date IS NOT NULL
        THEN CASE WHEN CAST(order_delivered_customer_date AS TIMESTAMP) <= CAST(order_estimated_delivery_date AS TIMESTAMP) THEN 1 ELSE 0 END
        ELSE NULL
    END AS is_on_time
FROM raw_orders
