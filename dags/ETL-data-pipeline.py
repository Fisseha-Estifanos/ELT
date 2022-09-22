# importing the required libraries
import sys
sys.path.append('..')
sys.path.insert(1, 'scripts/')
import defaults as defs
from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
# from airflow.providers.mysql.operators.mysql import MySqlOperator

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
dag = DAG(
    'ETl_data_pipeline',
    default_args=default_args,
    description='A data Extraction and loading pipeline for week 5 of 10 '
    + 'academy project',
    schedule=timedelta(days=1),     # run every hour
    catchup=False                   # dont perform a backfill of missing runs
)


def startProcess():
    print('Starting the extraction process')
    return 'Starting the extraction process'


# define steps
# entry point task - first task
entry_point = PythonOperator(
    task_id='task_initialization',
    python_callable=startProcess,
    dag=dag
)


# data extraction task - second task
extraction_command = f'wget {defs.path_to_source} -O {defs.path_to_store_data}'
extract_data = BashOperator(
    task_id='extraction',
    # bash_command='echo "BashOperator on Airflow" > bash1_op.txt',
    bash_command=extraction_command,
    dag=dag
)

#mysql1 = MySqlOperator(
 #   task_id='mysql1',
  #  mysql_conn_id='airflow_db',     # name of the connection we defined
   # sql="""INSERT INTO airflow_db.airflow_tb values (5);""",    # sql command;
    #dag=dag
#)

# entry_point >> extract_data >> mysql1
entry_point >> extract_data
print('ETL data pipeline DAG over and out')
