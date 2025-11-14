CREATE OR REPLACE TABLE silver.tickets AS
SELECT
    ticket_id,
    customer_external_id AS customer_id,
    order_id,
    channel,
    priority,
    status,
    category,
    subject,
    body AS description,
    sentiment,
    CAST(sla_due_at AS TIMESTAMP) AS sla_due_at,
    CAST(first_response_at AS TIMESTAMP) AS first_response_at,
    CAST(resolved_at AS TIMESTAMP) AS resolved_at,
    CAST(updated_at AS TIMESTAMP) AS ticket_ts,
    tags,
    agent_id,
    source_blob,
    loaded_at
FROM bronze.raw_tickets
WHERE ticket_id IS NOT NULL
ORDER BY ticket_id;
