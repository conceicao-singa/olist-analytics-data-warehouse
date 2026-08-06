-- Row count sanity: should be close to bronze_customers count
SELECT COUNT(*) AS row_count FROM silver_customers;

-- No null primary keys
SELECT COUNT(*) AS null_customer_id
FROM silver_customers
WHERE customer_id IS NULL;

-- customer_id should be unique (grain check — one row per customer_id, NOT per unique_id)
SELECT customer_id, COUNT(*) AS cnt
FROM silver_customers
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- customer_unique_id can repeat (same person, multiple orders) — sanity check this is expected
SELECT customer_unique_id, COUNT(*) AS cnt
FROM silver_customers
GROUP BY customer_unique_id
HAVING COUNT(*) > 1
ORDER BY cnt DESC
LIMIT 10;

-- State codes should all be valid 2-letter Brazilian state abbreviations
SELECT DISTINCT customer_state
FROM silver_customers
ORDER BY customer_state;