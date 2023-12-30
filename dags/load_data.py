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
def load_data():
        
    #load file
    path = "/opt/airflow/data/instacart-market-basket-analysis/"
    df = pd.read_csv(path + "products.csv")
    
    #connect sql engine to postgres
    engine = create_engine('postgresql://airflow:airflow@172.17.0.1:5432/airflow')

    #create out schema if not exists
    with engine.connect() as connection:
        connection.execute("CREATE SCHEMA IF NOT EXISTS instacart")

    #create table from file
    df.to_sql('products', engine, if_exists='replace', index=False, schema='instacart')

# Create the task
load_data_task = PythonOperator(
    task_id='load_data_task',
    python_callable=load_data,
    dag=dag,
)

# Set the task dependencies
load_data_task
