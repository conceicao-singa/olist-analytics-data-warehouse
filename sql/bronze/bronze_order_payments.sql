CREATE TABLE IF NOT EXISTS bronze_order_payments AS
SELECT *
FROM read_csv_auto('C:\\Users\\DELL\\projects\\Analytics\\olist_analytics-data_warehouse\\data\\raw\\olist_order_payments_dataset.csv');