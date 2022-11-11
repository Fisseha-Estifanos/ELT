"""
A test script to run a postgresql dag.
"""

# imports
import os
from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow import DAG
# create_pet_table, populate_pet_table, get_all_pets, and get_birth_date are
# examples of tasks created by instantiating the Postgres Operator

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "postgres_operator_dag"

default_args = {
    'owner': 'foxtrot',
    'depends_on_past': False,
    # 'start_date': days_ago(5),
    'start_date': datetime(2022, 9, 19),
    'email': ['fisseha.137@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    # schedule_interval="@daily",
    # schedule=timedelta(hours=1),      # run every hour
    schedule="@once",
    # catchup=False):
        catchup=False) as dag:

    create_pet_table = PostgresOperator(
        task_id="create_pet_table",
        postgres_conn_id="postgres_default",
        # sql="../sql/pet_schema.sql",
        sql="""
        -- create pet table
        CREATE TABLE IF NOT EXISTS pet (
        pet_id SERIAL PRIMARY KEY,
        name VARCHAR NOT NULL,
        pet_type VARCHAR NOT NULL,
        birth_date DATE NOT NULL,
        OWNER VARCHAR NOT NULL);
        """)

    populate_pet_table = PostgresOperator(
        task_id="populate_pet_table",
        postgres_conn_id="postgres_default",
        # sql="../sql/populate_pet_schema.sql",
        sql="""
        -- populate pet table
        INSERT INTO pet VALUES (1, 'Max', 'Dog', '2018-07-05', 'Jane');
        INSERT INTO pet VALUES (2, 'Susie', 'Cat', '2019-05-01', 'Phil');
        INSERT INTO pet VALUES (3, 'Lester', 'Hamster', '2020-06-23', 'Lily');
        INSERT INTO pet VALUES (4, 'Quincy', 'Parrot', '2013-08-11', 'Anne');
        """)

    get_all_pets = PostgresOperator(
        task_id="get_all_pets",
        postgres_conn_id="postgres_default",
        sql="SELECT * FROM pet;")

    get_birth_date = PostgresOperator(
        task_id="get_birth_date",
        postgres_conn_id="postgres_default",
        sql="SELECT * FROM pet WHERE birth_date BETWEEN SYMMETRIC " +
            "%(begin_date)s AND %(end_date)s",
        parameters={"begin_date": "2020-01-01", "end_date": "2020-12-31"})

create_pet_table >> populate_pet_table >> get_all_pets >> get_birth_date
print('postgreSQL test DAG over and out')
