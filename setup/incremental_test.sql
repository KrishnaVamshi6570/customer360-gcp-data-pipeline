-- Insert a new order to simulate fresh data arriving

INSERT INTO bronze_raw.orders_src (
  order_id,
  customer_id,
  order_status,
  order_purchase_timestamp,
  ingestion_timestamp,
  batch_id
)
VALUES (
  'test_order_10001',
  (SELECT customer_id FROM bronze_raw.customers_src LIMIT 1),
  'delivered',
  CURRENT_TIMESTAMP(),
  CURRENT_TIMESTAMP(),
  2
);


-- Insert a late-arriving order with an older business timestamp
-- but a new ingestion timestamp

INSERT INTO bronze_raw.orders_src (
  order_id,
  customer_id,
  order_status,
  order_purchase_timestamp,
  ingestion_timestamp,
  batch_id
)
VALUES (
  'test_order_20001',
  (SELECT customer_id FROM bronze_raw.customers_src LIMIT 1),
  'delivered',
  TIMESTAMP('2024-01-01 10:00:00'),
  CURRENT_TIMESTAMP(),
  2
);


-- Validate that incremental logic picks up the late-arriving row

SELECT *
FROM gold_marts.fct_orders
WHERE order_id = 'test_order_20001';