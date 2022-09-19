# importing the required libraries
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator


default_args = {
    'owner': 'foxtrot',
    'depends_on_past': False,
    'start_date': datetime(2022, 9, 19),
    'email': ['fisseha.137@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

# define the DAG
dag = DAG(
    'airflow_get_started',
    default_args=default_args,
    description='Getting Started with Airflow',
    schedule_interval=timedelta(hours=1),       # run every hour
    catchup=False           # do not perform a backfill of missing runs
)


def hello_world():
    print('Hello World')
    return 'This gets printed in the logs'


# define steps
python1 = PythonOperator(
    task_id='python1',
    python_callable=hello_world,
    dag=dag
)
bash1 = BashOperator(
    task_id='bash1',
    bash_command='echo "BashOperator on Airflow" > bash1_op.txt',
    dag=dag
)
mysql1 = MySqlOperator(
    task_id='mysql1',
    mysql_conn_id='airflow_db',     # name of the connection we defined
    sql="""INSERT INTO airflow_db.airflow_tb values (5);""",    # sql command;
    dag=dag
)

python1 >> bash1 >> mysql1
