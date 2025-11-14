CREATE OR REPLACE TABLE gold.metrics AS
WITH aov AS (
    SELECT AVG(order_total) AS average_order_value
    FROM gold.fact_orders
    WHERE order_total > 0
),
tickets AS (
    SELECT AVG(ticket_count) AS avg_tickets_per_order
    FROM gold.tickets_per_order
)
SELECT
    ROUND(aov.average_order_value, 2) AS average_order_value,
    ROUND(COALESCE(tickets.avg_tickets_per_order, 0), 4) AS avg_tickets_per_order
FROM aov
LEFT JOIN tickets ON TRUE;
