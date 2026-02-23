import duckdb

def get_order_dates(db_path="olist.duckdb"):
    conn = duckdb.connect(db_path)

    rows = conn.execute("""
        SELECT DISTINCT
            DATE(order_purchase_timestamp) AS order_date
        FROM silver_orders
        ORDER BY order_date
    """).fetchall()

    conn.close()

    # Convert to ISO strings (YYYY-MM-DD)
    return [row[0].isoformat() for row in rows]