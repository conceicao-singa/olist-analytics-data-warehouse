CREATE TABLE IF NOT EXISTS bronze_sellers AS
SELECT *
FROM read_csv_auto('C:\\Users\\DELL\\projects\\Analytics\\olist_analytics-data_warehouse\\data\\raw\\olist_sellers_dataset.csv');