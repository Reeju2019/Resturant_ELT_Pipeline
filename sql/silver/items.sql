CREATE OR REPLACE TABLE silver.items AS
SELECT
    id AS item_id,
    order_id,
    sku AS product_sku,
    loaded_at
FROM bronze.raw_items
WHERE id IS NOT NULL
  AND order_id IS NOT NULL
  AND sku IS NOT NULL
ORDER BY order_id, id;
