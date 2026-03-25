-- Create bronze_orders table from CSV
CREATE TABLE IF NOT EXISTS bronze_orders AS
SELECT *
FROM read_csv_auto('C:\\Users\\DELL\\projects\\Analytics\\olist_analytics-data_warehouse\\data\\raw\\olist_orders_dataset.csv');