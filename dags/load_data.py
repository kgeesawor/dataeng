from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
import io
#from minio import Minio

# Define the DAG
dag = DAG(
    'load_data',
    description='DAG to load data from CSV into PostgreSQL',
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
)

# Define the task to load data
def load_data(filename):
    chunk_size = 10000  # Set the desired chunk size
    
    # Load file in chunks
    path = "/opt/airflow/data/instacart-market-basket-analysis/"
    reader = pd.read_csv(path + filename + ".csv", chunksize=chunk_size)
    
    # Connect SQL engine to PostgreSQL
    engine = create_engine('postgresql://airflow:airflow@172.17.0.1:5432/airflow')

    # Create output schema if not exists
    with engine.connect() as connection:
        connection.execute("CREATE SCHEMA IF NOT EXISTS instacart")

    # Create table from chunks
    for i, chunk in enumerate(reader):
        table_name = f"{filename}"
        chunk.to_sql(table_name, engine, if_exists='append', index=False, schema='instacart')

# Create the task
load_data_task = PythonOperator(
    task_id='load_data_train',
    python_callable=load_data,
    op_kwargs={'filename': 'order_products__train'},
    dag=dag,
)

# Set the task dependencies
load_data_task
