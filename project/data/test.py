import duckdb, os
output_db_path = os.path.expanduser('~/airflow/data/output.db')
con = duckdb.connect(output_db_path)
print(con.execute("SHOW TABLES;").fetchall())
