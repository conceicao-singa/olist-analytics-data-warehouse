import duckdb

conn = duckdb.connect("olist.duckdb")  

with open("sql/quality_checks/silver/customer_checks.sql", "r") as f:
    sql_script = f.read()

raw_queries = [q.strip() for q in sql_script.split(";") if q.strip()]

for raw_q in raw_queries:
    clean_lines = [line for line in raw_q.splitlines() if not line.strip().startswith("--")]
    query = "\n".join(clean_lines).strip()

    if not query:
        continue

    print(f"\n--- Running ---\n{query}\n")
    result = conn.execute(query).fetchdf()
    print(result)

conn.close()