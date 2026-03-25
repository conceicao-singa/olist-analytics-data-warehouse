CREATE TABLE IF NOT EXISTS bronze_prod_cat_name AS
SELECT *
FROM read_csv_auto('C:\Users\\DELL\\projects\\Analytics\\olist_analytics-data_warehouse\\data\\raw\\product_category_name_translation.csv');