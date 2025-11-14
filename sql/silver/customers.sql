CREATE OR REPLACE TABLE silver.customers AS
SELECT DISTINCT
    id AS customer_id,
    name AS customer_name,
    loaded_at
FROM bronze.raw_customers
WHERE id IS NOT NULL
ORDER BY id;
