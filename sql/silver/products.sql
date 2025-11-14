CREATE OR REPLACE TABLE silver.products AS
SELECT DISTINCT
    sku AS product_sku,
    name AS product_name,
    type AS product_type,
    CAST(price AS DECIMAL(10,2)) AS product_price,
    description AS product_description,
    loaded_at
FROM bronze.raw_products
WHERE sku IS NOT NULL
ORDER BY sku;
