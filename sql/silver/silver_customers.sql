CREATE OR REPLACE TABLE silver_customers AS
WITH ranked AS (
    SELECT
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        LOWER(TRIM(customer_city)) AS customer_city,
        UPPER(TRIM(customer_state)) AS customer_state,
        ROW_NUMBER() OVER (
            PARTITION BY customer_unique_id 
            ORDER BY customer_id
        ) AS rn
    FROM bronze_customers
    WHERE customer_unique_id IS NOT NULL 
    AND customer_id IS NOT NULL
    AND customer_city IS NOT NULL
)
SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
FROM ranked
WHERE rn = 1;
