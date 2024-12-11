from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import pandas as pd
import duckdb

# Define the DuckDB database file path
db_path = './data/my_database.db'

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
file_path_new = 'data/Eesti haldus- ja asustusjaotuse klassifikaator 2024v2.xlsx'
file_path_last = 'data/PA107_20241206-142137.csv'
input_file_path_2023 = 'data/Ametnike_palgad_2023.xlsx'
input_file_path_2022 = 'data/Ametnike_palgad_2022.xlsx'
input_file_path_2021 = 'data/Ametnike_palgad_2021.xlsx'

# Clean and load data from the first Excel file (Eesti haldus- ja asustusjaotuse klassifikaator 2024v2.xlsx)
clean_and_load('Sheet1', 0, 1, 4802, 'klassifikaatorid', file_path_new)

# Clean and load data from the second Excel file (Ametnike_palgad.xlsx)
clean_and_load('KOV kogupalk 2023', 8, 10, 3970, 'kov_kogupalk_2023', input_file_path_2023)
clean_and_load('RIIK kogupalk 2023', 6, 8, 15143, 'riik_kogupalk_2023', input_file_path_2023)
clean_and_load('KOV_kogupalk 2022', 8, 10, 3971, 'kov_kogupalk_2022', input_file_path_2022)
clean_and_load('RIIK_kogupalk 2022', 7, 9, 13064, 'riik_kogupalk_2022', input_file_path_2022)
clean_and_load('KOV_kogupalk 2021', 7, 9, 3807, 'kov_kogupalk_2021', input_file_path_2021)
clean_and_load('RIIK_kogupalk 2021', 7, 9, 13049, 'riik_kogupalk_2021', input_file_path_2021)
# Clean and load data from the CSV file (PA107_20241206-142137.csv)
keskmised_näitajad = pd.read_csv(file_path_last, header=2)  # Loading CSV with header_row_last = 2

# Register the DataFrame as a temporary table in DuckDB
con.register('temp_keskmised_näitajad', keskmised_näitajad)

# Create a table in DuckDB from the registered DataFrame
table_name_keskmised_näitajad = "keskmised_näitajad"  # Table name in DuckDB
con.execute(f"CREATE TABLE IF NOT EXISTS {table_name_keskmised_näitajad} AS SELECT * FROM temp_keskmised_näitajad")

# Print confirmation message
print(f"Data successfully loaded into DuckDB table: {table_name_keskmised_näitajad}")

# Close the connection
con.close()
