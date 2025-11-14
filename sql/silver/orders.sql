CREATE OR REPLACE TABLE silver.orders AS
SELECT
    id AS order_id,
    customer AS customer_id,
    store_id,
    CAST(ordered_at AS TIMESTAMP) AS order_ts,
    CAST(subtotal AS DECIMAL(10,2)) AS subtotal,
    CAST(tax_paid AS DECIMAL(10,2)) AS tax_paid,
    CAST(order_total AS DECIMAL(10,2)) AS order_total,
    loaded_at
FROM bronze.raw_orders
WHERE id IS NOT NULL
  AND customer IS NOT NULL
ORDER BY id;
