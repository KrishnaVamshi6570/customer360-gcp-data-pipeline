-- Reduce source table sizes for cost optimization

CREATE OR REPLACE TABLE bronze_raw.orders_src_small AS
SELECT *
FROM bronze_raw.orders_src
LIMIT 5000;

CREATE OR REPLACE TABLE bronze_raw.customers_src_small AS
SELECT *
FROM bronze_raw.customers_src
LIMIT 1000;

CREATE OR REPLACE TABLE bronze_raw.order_items_src_small AS
SELECT *
FROM bronze_raw.order_items_src
LIMIT 8000;

CREATE OR REPLACE TABLE bronze_raw.payments_src_small AS
SELECT *
FROM bronze_raw.payments_src
LIMIT 5000;

CREATE OR REPLACE TABLE bronze_raw.products_src_small AS
SELECT *
FROM bronze_raw.products_src
LIMIT 1000;

-- Rebuild bronze tables from reduced tables and add ingestion metadata

CREATE OR REPLACE TABLE bronze_raw.customers_src AS
SELECT
  *,
  CURRENT_TIMESTAMP() AS ingestion_timestamp,
  1 AS batch_id
FROM bronze_raw.customers_src_small;

CREATE OR REPLACE TABLE bronze_raw.orders_src AS
SELECT
  *,
  CURRENT_TIMESTAMP() AS ingestion_timestamp,
  1 AS batch_id
FROM bronze_raw.orders_src_small;

CREATE OR REPLACE TABLE bronze_raw.order_items_src AS
SELECT
  *,
  CURRENT_TIMESTAMP() AS ingestion_timestamp,
  1 AS batch_id
FROM bronze_raw.order_items_src_small;

CREATE OR REPLACE TABLE bronze_raw.payments_src AS
SELECT
  *,
  CURRENT_TIMESTAMP() AS ingestion_timestamp,
  1 AS batch_id
FROM bronze_raw.payments_src_small;

CREATE OR REPLACE TABLE bronze_raw.products_src AS
SELECT
  *,
  CURRENT_TIMESTAMP() AS ingestion_timestamp,
  1 AS batch_id
FROM bronze_raw.products_src_small;

-- Verify reduced row counts after bronze rebuild

SELECT 'customers' AS table_name, COUNT(*) AS row_count
FROM bronze_raw.customers_src
UNION ALL
SELECT 'orders', COUNT(*)
FROM bronze_raw.orders_src
UNION ALL
SELECT 'order_items', COUNT(*)
FROM bronze_raw.order_items_src
UNION ALL
SELECT 'payments', COUNT(*)
FROM bronze_raw.payments_src
UNION ALL
SELECT 'products', COUNT(*)
FROM bronze_raw.products_src;

-- Drop temporary small tables after rebuild

DROP TABLE bronze_raw.customers_src_small;
DROP TABLE bronze_raw.orders_src_small;
DROP TABLE bronze_raw.order_items_src_small;
DROP TABLE bronze_raw.payments_src_small;
DROP TABLE bronze_raw.products_src_small;