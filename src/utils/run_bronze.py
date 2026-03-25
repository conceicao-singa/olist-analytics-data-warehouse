import duckdb
import glob
import pandas as pd



# Connect to DuckDB
conn = duckdb.connect("olist.duckdb")

# Function to run all SQL in a folder
def run_sql_folder(folder_path):
    sql_files = sorted(glob.glob(f"{folder_path}/*.sql"))
    for file in sql_files:
        with open(file, 'r') as f:
            sql = f.read()
            print(f"Running {file}...")
            conn.execute(sql)
