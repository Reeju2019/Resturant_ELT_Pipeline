CREATE OR REPLACE TABLE silver.stores AS
SELECT DISTINCT
    id AS store_id,
    name AS store_name,
    CAST(opened_at AS TIMESTAMP) AS opened_at,
    CAST(tax_rate AS DECIMAL(5,4)) AS tax_rate,
    loaded_at
FROM bronze.raw_stores
WHERE id IS NOT NULL
ORDER BY id;
