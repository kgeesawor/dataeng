from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine
import io
import os
import gc
        
#from minio import Minio

# Define the DAG
dag = DAG(
    'load_data_parquet',
    description='DAG to load data from parquet file into PostgreSQL',
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
)

# Define the task to load data
def load_data(filename, schemaName):        
    # Connect SQL engine to PostgreSQL
    engine = create_engine('postgresql://airflow:airflow@172.17.0.1:5432/airflow')

    # Create output schema if not exists
    with engine.connect() as connection:
        connection.execute(f"CREATE SCHEMA IF NOT EXISTS {schemaName}")

    # Create table from chunks for each file
    print("starting:" + filename)
    parquet_file = pq.ParquetFile(path + filename + ".parquet")
    
    # Total number of rows in the file
    total_rows = parquet_file.metadata.num_rows

    # Batch size    
    batch_size = 100000

    # Estimate the number of batches
    estimated_batches = total_rows // batch_size + (total_rows % batch_size > 0)
        
    # Load each piece one at a time
    i = 0
    for batch in parquet_file.iter_batches(batch_size = 100000):
        df = batch.to_pandas()
        table_name = f"{filename}"
        
        print("loading chunk:" + str(i) + " of " + str(estimated_batches))
        if i == 0:
            df.to_sql(table_name, engine, if_exists='replace', index=False, schema=schemaName)
        else:
            df.to_sql(table_name, engine, if_exists='append', index=False, schema=schemaName)
        
        del df
        gc.collect()
        i+=1

dataFolder = 'instacart-market-basket-analysis'
path = "/opt/airflow/data/" + dataFolder + "/"
filenames = [filename[:-8] for filename in os.listdir(path) if filename.endswith(".parquet")]

# Create the task
for filename in filenames:
    load_data_task = PythonOperator(
        task_id='load_data_'+filename,
        python_callable=load_data,
        op_kwargs={'filename':filename,'schemaName':'instacart'},
        dag=dag,
    )

