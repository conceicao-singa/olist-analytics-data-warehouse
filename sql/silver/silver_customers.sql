-- sql/silver/customers.sql

/*
Purpose:
    Clean customer master data (Bronze -> Silver).

Transformations:
    - Trim leading/trailing whitespace
    - Standardize city names to lowercase
    - Standardize state abbreviations to uppercase
    - Remove records with missing identifiers

NOT done here (belongs in Gold):
    - Deduplication by customer_unique_id
    - Any grain change (Silver stays at bronze_customers grain)

Layer: Silver
*/

CREATE OR REPLACE TABLE silver_customers AS
SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    LOWER(TRIM(customer_city))  AS customer_city,
    UPPER(TRIM(customer_state)) AS customer_state
FROM bronze_customers
WHERE customer_id IS NOT NULL
  AND customer_unique_id IS NOT NULL
  AND customer_city IS NOT NULL;