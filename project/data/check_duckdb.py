import duckdb

# Path to the database file inside Docker
db_path = './data/my_database.duckdb' # Adjust this path if needed

# Connect to DuckDB (file-based database)
con = duckdb.connect(database=db_path)

# Query the list of tables in the database
tables = con.execute("SHOW TABLES").fetchall()

# Print the tables
print("Tables in DuckDB:", tables)

# If there are tables, you can check the data from any table, for example:
if tables:
    table_name = tables[0][0]  # Get the first table name from the list
    data = con.execute(f"SELECT * FROM {table_name} LIMIT 5").fetchall()
    print(f"Data from {table_name}:", data)
else:
    print("No tables found.")

# Close the connection
con.close()
