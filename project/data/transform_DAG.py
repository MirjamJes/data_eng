from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import duckdb
import os

# Paths
db_path = os.path.expanduser('~/airflow/data/database.db')
final_db_path = os.path.expanduser('~/airflow/data/transform.db')

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
    'estonian_salary_transform_pipeline',
    default_args=default_args,
    description='ETL pipeline for Estonian salary data transformation',
    schedule='@once'
)

def init_database():
    try:
        print(f"Attempting to initialize database at: {db_path}")
        print(f"Current working directory: {os.getcwd()}")
        print("Files in airflow/data directory:")
        print(os.listdir(os.path.expanduser('~/airflow/data')))

        con = duckdb.connect(database=db_path)
        con.close()
        print("DB initialized successfully")
    except Exception as e:
        print(f"Error initializing database: {str(e)}")
        raise

def clean_and_load_sheet(**context):
    con = duckdb.connect(database=db_path)

    sheet_name = context['sheet_name']
    header_row = context['header_row']
    data_start_row = context['data_start_row']
    data_end_row = context['data_end_row']
    table_name = context['table_name']
    file_path = context['file_path']

    try:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Excel file not found at path: {file_path}")

        print(f"Processing sheet: {sheet_name}")
        print(f"Reading from file: {file_path}")

        df = pd.read_excel(file_path, sheet_name=sheet_name, header=header_row)
        df_cleaned = df.iloc[data_start_row:data_end_row + 1, :]

        con.register("temp_table", df_cleaned)
        con.execute(f"DROP TABLE IF EXISTS {table_name}")
        con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM temp_table")
        con.commit()

        con.unregister("temp_table")
        print(f"Successfully loaded {len(df_cleaned)} rows into table {table_name}")

    except Exception as e:
        print(f"Error processing {file_path}: {str(e)}")
        raise
    finally:
        con.close()

def process_csv_data():
    try:
        con = duckdb.connect(database=db_path)
        file_path_last = os.path.expanduser('~/airflow/data/PA107_20241206-142137.csv')

        if not os.path.exists(file_path_last):
            raise FileNotFoundError(f"CSV file not found at path: {file_path_last}")

        print(f"Reading CSV from: {file_path_last}")
        keskmised_näitajad = pd.read_csv(file_path_last, header=2)

        con.register('temp_keskmised_näitajad', keskmised_näitajad)
        table_name_keskmised_näitajad = "keskmised_näitajad"
        con.execute(f"CREATE TABLE IF NOT EXISTS {table_name_keskmised_näitajad} AS SELECT * FROM temp_keskmised_näitajad")
        print(f"Successfully loaded {len(keskmised_näitajad)} rows of CSV data")

    except Exception as e:
        print(f"Error processing CSV: {str(e)}")
        raise
    finally:
        con.close()

def parse_date(d_str):
    if not isinstance(d_str, str):
        return None
    parts = d_str.split('.')
    if len(parts) < 3:
        return None
    day, month, year_part = parts[0], parts[1], parts[2]
    if len(year_part) == 2:
        year = 2000 + int(year_part)
    else:
        try:
            year = int(year_part)
        except:
            return None
    try:
        return datetime(year, int(month), int(day))
    except:
        return None

def process_date_range(range_str):
    if not isinstance(range_str, str):
        return None
    # Remove spaces
    range_str = range_str.replace(' ', '')
    if range_str == '-':
        return None
    parts = range_str.split('-')
    if len(parts) != 2:
        return None
    start_str, end_str = parts[0], parts[1]
    start = parse_date(start_str)
    end = parse_date(end_str)
    if start is None or end is None:
        return None
    delta_days = (end - start).days
    if delta_days <= 0:
        return None
    fraction = delta_days / 365.0
    return fraction

def find_maakond_for_vald(vald, klass):
    # vald = "Anija vald"
    # klass['Description-et-EE'] examples: "Harju maakond, Anija vald, Aavere küla"
    # We need to find a row that matches ", Anija vald" in Description-et-EE and take the first part before comma as maakond.
    vald_stripped = vald.strip()
    candidate = klass[klass['Description-et-EE'].str.contains(fr",\s*{vald_stripped}\b", case=False, na=False)]
    if len(candidate) == 0:
        # Try also exact matches if needed
        return None
    desc = candidate.iloc[0]['Description-et-EE']
    parts = [x.strip() for x in desc.split(',')]
    if vald_stripped in parts:
        return parts[0]  # maakond should be the first element
    # If no exact match, just return the first element
    return parts[0]

def transform_kov_data():
    con = duckdb.connect(database=db_path)
    try:
        klass = con.execute("SELECT * FROM klassifikaatorid").df()
        if 'Description-et-EE' not in klass.columns:
            raise Exception("klassifikaatorid table missing 'Description-et-EE' column")

        kov_tables = ['kov_kogupalk_2021', 'kov_kogupalk_2022', 'kov_kogupalk_2023']
        all_kov = []

        for tbl in kov_tables:
            print(f"Processing {tbl}")
            df = con.execute(f"SELECT * FROM {tbl}").df()
            # year from name
            year = int(tbl[-4:])
            # kov: value in col K (index 10), date in col L (index 11), vald in col A (index 0)
            if len(df.columns) < 12:
                print(f"Table {tbl} does not have enough columns. Skipping.")
                continue
            value_col = df.columns[10]
            date_col = df.columns[11]
            vald_col = df.columns[0]

            # Drop rows without value or date
            df = df.dropna(subset=[value_col, date_col])
            df[value_col] = pd.to_numeric(df[value_col], errors='coerce')
            df = df.dropna(subset=[value_col])

            df['fraction_of_year'] = df[date_col].apply(process_date_range)
            df = df.dropna(subset=['fraction_of_year'])

            df['transformed_value'] = df[value_col] / df['fraction_of_year']
            df['year'] = year

            # Find maakond
            df['vald'] = df[vald_col].astype(str).str.strip()
            df['maakond'] = df['vald'].apply(lambda v: find_maakond_for_vald(v, klass))
            df = df.dropna(subset=['maakond'])

            df_kov = df[['year', 'maakond', 'transformed_value']]
            all_kov.append(df_kov)

        if all_kov:
            kov_final = pd.concat(all_kov, ignore_index=True)
        else:
            kov_final = pd.DataFrame(columns=['year', 'maakond', 'transformed_value'])

        # Store intermediate result in database.db
        con.execute("DROP TABLE IF EXISTS kov_transformed")
        con.register("temp_kov_transformed", kov_final)
        con.execute("CREATE TABLE kov_transformed AS SELECT * FROM temp_kov_transformed")
        con.unregister("temp_kov_transformed")
        con.commit()

        print(f"Transformed KOV data stored in kov_transformed with {len(kov_final)} rows.")

    except Exception as e:
        print(f"Error in transform_kov_data: {str(e)}")
        raise
    finally:
        con.close()

def transform_riik_data():
    con = duckdb.connect(database=db_path)
    try:
        riik_tables = ['riik_kogupalk_2021', 'riik_kogupalk_2022', 'riik_kogupalk_2023']
        all_riik = []

        for tbl in riik_tables:
            print(f"Processing {tbl}")
            df = con.execute(f"SELECT * FROM {tbl}").df()
            # year from name
            year = int(tbl[-4:])
            # riik: value in col J (index 9), date in col K (index 10)
            if len(df.columns) < 11:
                print(f"Table {tbl} does not have enough columns. Skipping.")
                continue
            value_col = df.columns[9]
            date_col = df.columns[10]

            df = df.dropna(subset=[value_col, date_col])
            df[value_col] = pd.to_numeric(df[value_col], errors='coerce')
            df = df.dropna(subset=[value_col])

            df['fraction_of_year'] = df[date_col].apply(process_date_range)
            df = df.dropna(subset=['fraction_of_year'])

            df['transformed_value'] = df[value_col] / df['fraction_of_year']
            df['year'] = year
            df['maakond'] = 'Riik'

            df_riik = df[['year', 'maakond', 'transformed_value']]
            all_riik.append(df_riik)

        if all_riik:
            riik_final = pd.concat(all_riik, ignore_index=True)
        else:
            riik_final = pd.DataFrame(columns=['year', 'maakond', 'transformed_value'])

        # Store intermediate result in database.db
        con.execute("DROP TABLE IF EXISTS riik_transformed")
        con.register("temp_riik_transformed", riik_final)
        con.execute("CREATE TABLE riik_transformed AS SELECT * FROM temp_riik_transformed")
        con.unregister("temp_riik_transformed")
        con.commit()

        print(f"Transformed RIIK data stored in riik_transformed with {len(riik_final)} rows.")

    except Exception as e:
        print(f"Error in transform_riik_data: {str(e)}")
        raise
    finally:
        con.close()

def combine_data():
    # Combine kov_transformed and riik_transformed into transform.db:final_data
    # Also copy keskmised_näitajad to transform.db
    con = duckdb.connect(database=db_path)
    try:
        kov_exists = con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name='kov_transformed'").fetchone()[0] > 0
        riik_exists = con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name='riik_transformed'").fetchone()[0] > 0
        keskmised_exists = con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name='keskmised_näitajad'").fetchone()[0] > 0

        kov_df = pd.DataFrame(columns=['year', 'maakond', 'transformed_value'])
        riik_df = pd.DataFrame(columns=['year', 'maakond', 'transformed_value'])
        keskmised_n = pd.DataFrame()

        if kov_exists:
            kov_df = con.execute("SELECT * FROM kov_transformed").df()
        if riik_exists:
            riik_df = con.execute("SELECT * FROM riik_transformed").df()
        if keskmised_exists:
            keskmised_n = con.execute("SELECT * FROM keskmised_näitajad").df()

        final_df = pd.concat([kov_df, riik_df], ignore_index=True)

        # Write final_df and keskmised_näitajad to transform.db
        if os.path.exists(final_db_path):
            os.remove(final_db_path)
        out_con = duckdb.connect(database=final_db_path)
        out_con.execute("DROP TABLE IF EXISTS final_data")
        out_con.register("temp_final", final_df)
        out_con.execute("CREATE TABLE final_data AS SELECT * FROM temp_final")
        out_con.unregister("temp_final")

        if not keskmised_n.empty:
            out_con.execute("DROP TABLE IF EXISTS keskmised_näitajad")
            out_con.register("temp_keskmised", keskmised_n)
            out_con.execute("CREATE TABLE keskmised_näitajad AS SELECT * FROM temp_keskmised")
            out_con.unregister("temp_keskmised")

        out_con.close()
        print(f"Final transformed data and keskmised_näitajad written to {final_db_path}.")

    except Exception as e:
        print(f"Error in combine_data: {str(e)}")
        raise
    finally:
        con.close()


# Task definitions
init_db_task = PythonOperator(
    task_id='init_database',
    python_callable=init_database,
    dag=dag
)

load_kov_2023 = PythonOperator(
    task_id='load_kov_2023',
    python_callable=clean_and_load_sheet,
    op_kwargs={
        'sheet_name': 'KOV kogupalk 2023',
        'header_row': 8,
        'data_start_row': 10,
        'data_end_row': 3970,
        'table_name': 'kov_kogupalk_2023',
        'file_path': os.path.expanduser('~/airflow/data/Ametnike_palgad_2023.xlsx')
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
        'file_path': os.path.expanduser('~/airflow/data/Ametnike_palgad_2023.xlsx')
    },
    dag=dag
)

load_kov_2022 = PythonOperator(
    task_id='load_kov_2022',
    python_callable=clean_and_load_sheet,
    op_kwargs={
        'sheet_name': 'KOV_kogupalk 2022',
        'header_row': 8,
        'data_start_row': 10,
        'data_end_row': 3971,
        'table_name': 'kov_kogupalk_2022',
        'file_path': os.path.expanduser('~/airflow/data/Ametnike_palgad_2022.xlsx')
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
        'file_path': os.path.expanduser('~/airflow/data/Ametnike_palgad_2022.xlsx')
    },
    dag=dag
)

load_kov_2021 = PythonOperator(
    task_id='load_kov_2021',
    python_callable=clean_and_load_sheet,
    op_kwargs={
        'sheet_name': 'KOV_kogupalk 2021',
        'header_row': 7,
        'data_start_row': 9,
        'data_end_row': 3807,
        'table_name': 'kov_kogupalk_2021',
        'file_path': os.path.expanduser('~/airflow/data/Ametnike_palgad_2021.xlsx')
    },
    dag=dag 
)

load_riik_2021 = PythonOperator(
    task_id='load_riik_2021',
    python_callable=clean_and_load_sheet,
    op_kwargs={
        'sheet_name': 'RIIK_kogupalk_2021',
        'header_row': 7,
        'data_start_row': 9,
        'data_end_row': 13049,
        'table_name': 'riik_kogupalk_2021',
        'file_path': os.path.expanduser('~/airflow/data/Ametnike_palgad_2021.xlsx')
    },
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
        'file_path': os.path.expanduser('~/airflow/data/klassifikaator.xlsx')
    },
    dag=dag
)

load_csv_data = PythonOperator(
    task_id='load_csv_data',
    python_callable=process_csv_data,
    dag=dag
)

transform_kov_task = PythonOperator(
    task_id='transform_kov_data',
    python_callable=transform_kov_data,
    dag=dag
)

transform_riik_task = PythonOperator(
    task_id='transform_riik_data',
    python_callable=transform_riik_data,
    dag=dag
)

combine_task = PythonOperator(
    task_id='combine_data',
    python_callable=combine_data,
    dag=dag
)

# Dependencies
init_db_task >> [load_kov_2023, load_riik_2023, load_kov_2022, load_riik_2022, load_kov_2021, load_riik_2021]
[load_kov_2023, load_riik_2023, load_kov_2022, load_riik_2022, load_kov_2021, load_riik_2021] >> load_klassifikaatorid
load_klassifikaatorid >> load_csv_data >> [transform_kov_task, transform_riik_task] >> combine_task
