CREATE OR REPLACE TABLE gold.fact_orders AS
SELECT
    o.order_id,
    o.customer_id,
    o.store_id,
    o.order_ts,
    DATE_TRUNC('day', o.order_ts) AS order_date,
    o.subtotal,
    o.tax_paid,
    o.order_total,
    COUNT(i.item_id) AS item_count
FROM silver.orders o
LEFT JOIN silver.items i ON i.order_id = o.order_id
GROUP BY o.order_id, o.customer_id, o.store_id, o.order_ts, o.subtotal, o.tax_paid, o.order_total
ORDER BY o.order_id;
