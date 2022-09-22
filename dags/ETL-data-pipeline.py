# importing required libraries
import os
import sys
sys.path.append('..')
sys.path.insert(1, 'scripts/')
import defaults as defs
from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
from dotenv import load_dotenv
import pandas as pd

load_dotenv()
# get connection string
connection_string = os.getenv('conn_string')

default_args = {
    'owner': 'foxtrot',
    'depends_on_past': False,
    # 'start_date': days_ago(5),
    'start_date': datetime(2022, 9, 22),
    'email': ['fisseha.137@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
    'tags': ['week5', 'traffic data']
}

# define the DAG
etl_dag = DAG(
    'ETl_data_pipeline',
    default_args=default_args,
    description='A data Extraction and loading pipeline for week 5 of 10 '
    + 'academy project',
    schedule=timedelta(days=1),     # run every day
    catchup=False                   # dont perform a backfill of missing runs
)


# TODO: refactor this handler to an ETL separate script
def startProcess():
    """
    Initial task
    """
    print('Starting the extraction process')
    return 'Starting the extraction process'


# TODO: refactor this handler to an ETL separate script
def loadDataToDWH():
    """
    A data loader handler.
    Loads data from source csv file to postgreSQL DWH
    """
    print('starting data loading task')
    try:
        # create connection to database
        alchemyEngine = create_engine(connection_string)
        dbConnection = alchemyEngine.connect()

        # read the extracted data set
        df = pd.read_csv(defs.data_file)
        df.info()
        print(f'shape: {df.shape}')
        print(f'columns: {df.columns}')

        # the table to load to
        tableName = 'raw_data'
        frame_ = df.to_sql(tableName, dbConnection, index=False)
    except ValueError as vx:
        print(vx)
    except Exception as e:
        print(e)
    else:
        print(f"PostgreSQL Table '{tableName}' has been created successfully.")
    finally:
        # Close the database connection
        dbConnection.close()
        return 'data loading completed'


# TODO: refactor this handler to an ETL separate script
def organizeCols():
    """
    A data column organize handler.
    Organize the columns of the data set properly
    """
    print('starting loading data from DWH . . .')
    try:
        # create connection to database
        alchemyEngine = create_engine(connection_string)
        dbConnection = alchemyEngine.connect()

        data = pd.read_sql("select * from raw_data", dbConnection)
        print('data loaded from warehouse successfully')

        print('starting column organization . . .')
        data.info()
        print(f'shape: {data.shape}')
        print(f'columns: {data.columns}')
    except ValueError as vx:
        print(vx)
    except Exception as e:
        print(e)
    else:
        print("column organized successfully")
    finally:
        # Close the database connection
        dbConnection.close()
        return 'column organization completed'


# TODO: refactor this handler to an ETL separate script
# define steps
# entry point task - first task
entry_point = PythonOperator(
    task_id='task_initialization',
    python_callable=startProcess,
    dag=etl_dag
)

# data extraction task - second task
extraction_command = f'wget {defs.path_to_source} -O {defs.path_to_store_data}'
extract_data = BashOperator(
    task_id='extraction',
    bash_command=extraction_command,
    dag=etl_dag
)

# data loading task - third task
load_data_to_postgreSQL_DWH = PythonOperator(
    task_id='load_data_to_postgreSQL_DWH',
    python_callable=loadDataToDWH,
    dag=etl_dag
)

# TODO: refactor this handler to an ETL separate script
# data column organizer task - final task
organize_columns = PythonOperator(
    task_id='organize_data_columns',
    python_callable=organizeCols,
    dag=etl_dag
)

# entry_point >> extract_data >> mysql1
entry_point >> extract_data >> load_data_to_postgreSQL_DWH >> organize_columns

loadDataToDWH()
#organizeCols()

print('ETL data pipeline DAG over and out')
# 0   track_id; type; traveled_d; avg_speed; lat; lon; speed; lon_acc; lat_acc; time  922 non-null    object

# 0   index                                                            922 non-null    int64 
#  1   track_id; type; traveled_d; avg_speed; lat; lon; speed; lon_acc  922 non-null    object