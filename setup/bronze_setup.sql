-- Verify raw table uploads into bronze_raw

SELECT COUNT(*) AS customer_count
FROM bronze_raw.customers_src;

SELECT COUNT(*) AS orders_count
FROM bronze_raw.orders_src;

SELECT COUNT(*) AS order_items_count
FROM bronze_raw.order_items_src;

SELECT COUNT(*) AS payments_count
FROM bronze_raw.payments_src;

SELECT COUNT(*) AS products_count
FROM bronze_raw.products_src;