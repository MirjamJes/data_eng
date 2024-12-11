from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import duckdb
import os
import logging

final_db_path = os.path.expanduser('~/airflow/data/transform.db')
output_db_path = os.path.expanduser('~/airflow/data/output.db')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'estonian_salary_star_schema_simple',
    default_args=default_args,
    description='Create final star schema (simplified)',
    schedule_interval='@once',
    catchup=False
)


def create_star_schema(**kwargs):
    # Connect to final DB and load data
    con = duckdb.connect(database=final_db_path)
    final_data = con.execute("SELECT * FROM final_data").df()
    keskmised = con.execute("SELECT * FROM keskmised_näitajad").df()
    con.close()

    # Perform transformations
    agg = (
        final_data.groupby(['year', 'maakond'], as_index=False)
        .agg(mean_value=('transformed_value', 'mean'), count=('transformed_value', 'count'))
    )
    agg['brutopalk'] = agg['mean_value']/12
    agg['riigitöötajad'] = 'jah'
    agg = agg[['year', 'maakond', 'brutopalk', 'count', 'riigitöötajad']]

    # Select first 11 columns of keskmised and rename
    keskmised = keskmised.iloc[:, :11]
    keskmised.columns = [
        'maakond', 'brutopalk_2021', 'count_2021', 'i1', 'i2',
        'brutopalk_2022', 'count_2022', 'i3', 'i4',
        'brutopalk_2023', 'count_2023'
    ]

    # Transform keskmised to long format
    df_21 = keskmised[['maakond', 'brutopalk_2021', 'count_2021']].copy()
    df_21['year'] = 2021
    df_21.rename(columns={'brutopalk_2021': 'brutopalk', 'count_2021': 'count'}, inplace=True)

    df_22 = keskmised[['maakond', 'brutopalk_2022', 'count_2022']].copy()
    df_22['year'] = 2022
    df_22.rename(columns={'brutopalk_2022': 'brutopalk', 'count_2022': 'count'}, inplace=True)

    df_23 = keskmised[['maakond', 'brutopalk_2023', 'count_2023']].copy()
    df_23['year'] = 2023
    df_23.rename(columns={'brutopalk_2023': 'brutopalk', 'count_2023': 'count'}, inplace=True)

    keskmised_long = pd.concat([df_21, df_22, df_23], ignore_index=True)
    keskmised_long['riigitöötajad'] = 'ei'

    # Combine
    final_star = pd.concat([agg, keskmised_long], ignore_index=True)
    final_star['riigitöötajad'] = final_star['riigitöötajad'].replace({'jah': 'yes', 'ei': 'no'})
    final_star.rename(columns={
        'maakond': 'county',
        'brutopalk': 'bruto salary',
        'riigitöötajad': 'civil servant'
    }, inplace=True)

    # Write to output
    if os.path.exists(output_db_path):
        os.remove(output_db_path)
    out_con = duckdb.connect(database=output_db_path)
    out_con.execute("DROP TABLE IF EXISTS star_schema_final")
    out_con.register("temp_star", final_star)
    out_con.execute("CREATE TABLE star_schema_final AS SELECT * FROM temp_star")
    out_con.unregister("temp_star")
    out_con.close()


create_star_schema_task = PythonOperator(
    task_id='create_star_schema',
    python_callable=create_star_schema,
    dag=dag
)
