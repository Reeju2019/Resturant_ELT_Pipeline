CREATE OR REPLACE TABLE silver.supplies AS
SELECT
    id AS supply_id,
    name AS supply_name,
    CAST(cost AS DECIMAL(10,2)) AS supply_cost,
    perishable,
    sku AS product_sku,
    loaded_at
FROM bronze.raw_supplies
WHERE id IS NOT NULL
  AND sku IS NOT NULL
ORDER BY id;
