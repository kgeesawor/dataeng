from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
import io
import os
#from minio import Minio

# Define the DAG
dag = DAG(
    'load_data',
    description='DAG to load data from CSV into PostgreSQL',
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
)

# Define the task to load data
def load_data(dataFolder, schemaName):
    chunk_size = 100000  # Set the desired chunk size
    
    # Load file in chunks
    path = "/opt/airflow/data/" + dataFolder + "/"
    filenames = [filename[:-4] for filename in os.listdir(path) if filename.endswith(".csv")]
    
    # Connect SQL engine to PostgreSQL
    engine = create_engine('postgresql://airflow:airflow@172.17.0.1:5432/airflow')

    # Create output schema if not exists
    with engine.connect() as connection:
        connection.execute(f"CREATE SCHEMA IF NOT EXISTS {schemaName}")

    # Create table from chunks for each file
    for filename in filenames:
        reader = pd.read_parquet(path + filename + ".csv", chunksize=chunk_size)
        for i, chunk in enumerate(reader):
            table_name = f"{filename}"
            if i==0:            
                chunk.to_sql(table_name, engine, if_exists='replace', index=False, schema=schemaName)
            else:
                chunk.to_sql(table_name, engine, if_exists='append', index=False, schema=schemaName)

# Create the task
load_data_task = PythonOperator(
    task_id='load_data_instacart',
    python_callable=load_data,
    op_kwargs={'dataFolder': 'instacart-market-basket-analysis','schemaName':'instacart'},
    dag=dag,
)

# Set the task dependencies
load_data_task
