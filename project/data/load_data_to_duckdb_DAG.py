from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import duckdb
import os

db_path = './data/database.db'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'estonian_salary_data_pipeline',
    default_args=default_args,
    description='ETL pipeline for Estonian salary data',
    schedule_interval=None
)

def init_database():
    con = duckdb.connect(database=db_path)
    con.close()
    print("DB initialized")

def clean_and_load_sheet(**context):
    con = duckdb.connect(database=db_path)
    
    sheet_name = context['sheet_name']
    header_row = context['header_row']
    data_start_row = context['data_start_row']
    data_end_row = context['data_end_row']
    table_name = context['table_name']
    file_path = context['file_path']
    
    print(f"Processing sheet: {sheet_name}")
    
    df = pd.read_excel(file_path, sheet_name=sheet_name, header=header_row)
    df_cleaned = df.iloc[data_start_row:data_end_row + 1, :]
    
    con.register("temp_table", df_cleaned)
    con.execute(f"DROP TABLE IF EXISTS {table_name}")
    con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM temp_table")
    con.commit()
    
    con.unregister("temp_table")
    con.close()

def process_csv_data():
    con = duckdb.connect(database=db_path)
    file_path_last = 'data/PA107_20241206-142137.csv'
    keskmised_näitajad = pd.read_csv(file_path_last, header=2)
    
    con.register('temp_keskmised_näitajad', keskmised_näitajad)
    table_name_keskmised_näitajad = "keskmised_näitajad"
    con.execute(f"CREATE TABLE IF NOT EXISTS {table_name_keskmised_näitajad} AS SELECT * FROM temp_keskmised_näitajad")
    con.close()

# Task definitions
init_db_task = PythonOperator(
    task_id='init_database',
    python_callable=init_database,
    dag=dag
)

load_klassifikaatorid = PythonOperator(
    task_id='load_klassifikaatorid',
    python_callable=clean_and_load_sheet,
    op_kwargs={
        'sheet_name': 'Sheet1',
        'header_row': 0,
        'data_start_row': 1,
        'data_end_row': 4802,
        'table_name': 'klassifikaatorid',
        'file_path': 'data/Eesti haldus- ja asustusjaotuse klassifikaator 2024v2.xlsx'
    },
    dag=dag
)

# 2023 data tasks
load_kov_2023 = PythonOperator(
    task_id='load_kov_2023',
    python_callable=clean_and_load_sheet,
    op_kwargs={
        'sheet_name': 'KOV kogupalk 2023',
        'header_row': 8,
        'data_start_row': 10,
        'data_end_row': 3970,
        'table_name': 'kov_kogupalk_2023',
        'file_path': 'data/Ametnike_palgad_2023.xlsx'
    },
    dag=dag
)

load_riik_2023 = PythonOperator(
    task_id='load_riik_2023',
    python_callable=clean_and_load_sheet,
    op_kwargs={
        'sheet_name': 'RIIK kogupalk 2023',
        'header_row': 6,
        'data_start_row': 8,
        'data_end_row': 15143,
        'table_name': 'riik_kogupalk_2023',
        'file_path': 'data/Ametnike_palgad_2023.xlsx'
    },
    dag=dag
)

# 2022 data tasks
load_kov_2022 = PythonOperator(
    task_id='load_kov_2022',
    python_callable=clean_and_load_sheet,
    op_kwargs={
        'sheet_name': 'KOV_kogupalk 2022',
        'header_row': 8,
        'data_start_row': 10,
        'data_end_row': 3971,
        'table_name': 'kov_kogupalk_2022',
        'file_path': 'data/Ametnike_palgad_2022.xlsx'
    },
    dag=dag
)

load_riik_2022 = PythonOperator(
    task_id='load_riik_2022',
    python_callable=clean_and_load_sheet,
    op_kwargs={
        'sheet_name': 'RIIK_kogupalk 2022',
        'header_row': 7,
        'data_start_row': 9,
        'data_end_row': 13064,
        'table_name': 'riik_kogupalk_2022',
        'file_path': 'data/Ametnike_palgad_2022.xlsx'
    },
    dag=dag
)

# 2021 data tasks
load_kov_2021 = PythonOperator(
    task_id='load_kov_2021',
    python_callable=clean_and_load_sheet,
    op_kwargs={
        'sheet_name': 'KOV_kogupalk 2021',
        'header_row': 7,
        'data_start_row': 9,
        'data_end_row': 3807,
        'table_name': 'kov_kogupalk_2021',
        'file_path': 'data/Ametnike_palgad_2021.xlsx'
    },
    dag=dag
)

load_riik_2021 = PythonOperator(
    task_id='load_riik_2021',
    python_callable=clean_and_load_sheet,
    op_kwargs={
        'sheet_name': 'RIIK_kogupalk 2021',
        'header_row': 7,
        'data_start_row': 9,
        'data_end_row': 13049,
        'table_name': 'riik_kogupalk_2021',
        'file_path': 'data/Ametnike_palgad_2021.xlsx'
    },
    dag=dag
)

load_csv_data = PythonOperator(
    task_id='load_csv_data',
    python_callable=process_csv_data,
    dag=dag
)

# Set up task dependencies
init_db_task >> load_klassifikaatorid
load_klassifikaatorid >> [load_kov_2023, load_riik_2023, load_kov_2022, load_riik_2022, load_kov_2021, load_riik_2021]
[load_kov_2023, load_riik_2023, load_kov_2022, load_riik_2022, load_kov_2021, load_riik_2021] >> load_csv_data