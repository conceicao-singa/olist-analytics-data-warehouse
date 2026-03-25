CREATE TABLE IF NOT EXISTS bronze_geolocation AS
SELECT *
FROM read_csv_auto('C:\\Users\\DELL\\projects\\Analytics\\olist_analytics-data_warehouse\\data\\raw\\olist_geolocation_dataset.csv');