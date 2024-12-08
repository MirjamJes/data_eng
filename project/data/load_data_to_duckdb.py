import os
import pandas as pd
import duckdb

# Define the DuckDB database file path
db_path = './data/my_database.duckdb'

# Connect to DuckDB (file-based or in-memory)
con = duckdb.connect(database=db_path)

# Function to clean and load data into DuckDB
def clean_and_load(sheet_name, header_row, data_start_row, data_end_row, table_name, file_path):
    print(f"Processing sheet: {sheet_name}")
    
    # Load the sheet with the specified header
    df = pd.read_excel(file_path, sheet_name=sheet_name, header=header_row)
    
    # Slice the relevant rows (data only)
    df_cleaned = df.iloc[data_start_row:data_end_row + 1, :]
    print(f"Loaded {df_cleaned.shape[0]} rows into {table_name}")
    
    # Register the DataFrame as a temporary table in DuckDB
    con.register("temp_table", df_cleaned)
    
    # Drop the table if it exists and create a new one from the registered table
    con.execute(f"DROP TABLE IF EXISTS {table_name}")
    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM temp_table")
    
    # Commit changes explicitly (not always necessary for DuckDB, but can help ensure persistence)
    con.commit()
    
    # Check if the table was created
    result = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchall()
    print(f"Table {table_name} created with {result[0][0]} rows.")
    
    # Unregister the temporary table
    con.unregister("temp_table")
    print(f"Table {table_name} created successfully.")

# File paths for each dataset
file_path_new = './data/Eesti haldus- ja asustusjaotuse klassifikaator 2024v2.xlsx'
file_path_last = './data/PA107_20241206-142137.csv'
input_file_path = './data/Ametnike_palgad.xlsx'

# Clean and load data from the first Excel file (Eesti haldus- ja asustusjaotuse klassifikaator 2024v2.xlsx)
clean_and_load('Sheet1', 0, 1, 4802, 'klassifikaatorid', file_path_new)

# Clean and load data from the second Excel file (Ametnike_palgad.xlsx)
clean_and_load('KOV 01.04.2024 põhipalk', 7, 9, 3369, 'kov_2024', input_file_path)
clean_and_load('KOV kogupalk 2023', 8, 10, 3970, 'kov_kogupalk_2023', input_file_path)
clean_and_load('RIIK 01.04.2024 põhipalk', 3, 5, 11367, 'riik_2024', input_file_path)
clean_and_load('RIIK kogupalk 2023', 6, 8, 15143, 'riik_kogupalk_2023', input_file_path)

# Clean and load data from the CSV file (PA107_20241206-142137.csv)
keskmised_näitajad = pd.read_csv(file_path_last, header=2)  # Loading CSV with header_row_last = 2
keskmised_näitajad_cleaned = keskmised_näitajad.iloc[3:76, :]  # Slice the relevant rows (data only)

# Register the DataFrame as a temporary table in DuckDB
con.register('temp_keskmised_näitajad', keskmised_näitajad_cleaned)

# Create a table in DuckDB from the registered DataFrame
table_name_keskmised_näitajad = "keskmised_näitajad"  # Table name in DuckDB
con.execute(f"CREATE TABLE IF NOT EXISTS {table_name_keskmised_näitajad} AS SELECT * FROM temp_keskmised_näitajad")

# Print confirmation message
print(f"Data successfully loaded into DuckDB table: {table_name_keskmised_näitajad}")

# Close the connection
con.close()
