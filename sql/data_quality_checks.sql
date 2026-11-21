-- Replace database and table names with your Glue Catalog names.

-- 1. Check for duplicate order IDs.
SELECT order_id, COUNT(*) AS record_count
FROM analytics.orders_curated
GROUP BY order_id
HAVING COUNT(*) > 1;

-- 2. Check required values and valid measures.
SELECT COUNT(*) AS invalid_record_count
FROM analytics.orders_curated
WHERE order_id IS NULL
   OR customer_id IS NULL
   OR quantity <= 0
   OR unit_price < 0
   OR gross_amount < 0;

-- 3. Daily order volume and revenue.
SELECT order_date,
       COUNT(*) AS order_count,
       SUM(gross_amount) AS gross_revenue
FROM analytics.orders_curated
GROUP BY order_date
ORDER BY order_date;

-- 4. Revenue by country.
SELECT country,
       COUNT(*) AS order_count,
       SUM(gross_amount) AS gross_revenue
FROM analytics.orders_curated
GROUP BY country
ORDER BY gross_revenue DESC;
