CREATE OR REPLACE TABLE gold.tickets_per_order AS
SELECT
    order_id,
    COUNT(*) AS ticket_count
FROM silver.tickets
WHERE order_id IS NOT NULL
GROUP BY order_id
ORDER BY order_id;
