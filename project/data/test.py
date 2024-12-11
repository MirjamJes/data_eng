import duckdb, os
db_path = os.path.expanduser('~/airflow/data/database.db')
con = duckdb.connect(db_path)
print(con.execute("SHOW TABLES;").fetchall())
