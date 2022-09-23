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
connection_string = os.getenv('postgreSQL_conn_string')

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
    print('Starting the extraction process . . .')
    return 'Starting the extraction process'


# TODO: refactor this handler to an ETL separate script
def loadDataToDWH():
    """
    A data loader handler.
    Loads data from source csv file to postgreSQL DWH
    """
    print('\nstarting loading data to the DWH . . .')
    try:
        # create connection to database
        alchemyEngine = create_engine(connection_string)
        dbConnection = alchemyEngine.connect()

        # read the extracted data set
        df = pd.read_csv(defs.data_file)
        df.info()
        print(f'shape: {df.shape}')
        print(f'columns: {df.columns}')

        # preparing the data before storing it to the DWH
        columns = df.columns[0].split(";")
        columns_ = []
        for cols in columns:
            columns_.append(str.strip(cols))
        columns = columns_
        columns.append('trackings')

        # setting up lists for each column
        track_ids = []
        types = []
        traveled_d = []
        avg_speeds = []
        lat = []
        lon = []
        speed = []
        lon_acc = []
        lat_acc = []
        time = []

        trackings = []
        listOfRows = []

        for r in range(len(df)):
            row = df.iloc[r, :][0].split(";")
            listOfRows.append(row)
            base_row = row[:10]
            tracking_row = row[10:]
            tracking = ','.join(tracking_row)

            track_ids.append(base_row[0])
            types.append(base_row[1])
            traveled_d.append(base_row[2])
            avg_speeds.append(base_row[3])
            lat.append(base_row[4])
            lon.append(base_row[5])
            speed.append(base_row[6])
            lon_acc.append(base_row[7])
            lat_acc.append(base_row[8])
            time.append(base_row[9])

            trackings.append(tracking[1:])

        # print the total number of prepared records
        print(f'total number of records prepared: {len(listOfRows)}')

        # prepare the data as a data frame format to load into the DWH
        base_data = {columns[0]: track_ids, columns[1]: types,
                     columns[2]: traveled_d, columns[3]: avg_speeds,
                     columns[4]: lat, columns[5]: lon, columns[6]: speed,
                     columns[7]: lon_acc, columns[8]: lat_acc,
                     columns[9]: time, columns[10]: trackings}

        # crate the data frame
        new_df = pd.DataFrame(base_data)

        # the table to load to
        tableName = 'raw_table'
        new_df.to_sql(tableName, dbConnection, index=False)
    except ValueError as vx:
        print(vx)
    except Exception as e:
        print(e)
    else:
        print(f"PostgreSQL table '{tableName}' has been created successfully.")
    finally:
        # Close the database connection
        dbConnection.close()
        return 'data loaded to DWH successfully completed'


# TODO: refactor this handler to an ETL separate script
def organizeTables():
    """
    A data table organize handler.
    Organize the columns of the data set properly into two tables
    """
    print('\nstarting loading data from DWH . . .')
    try:
        # create connection to database
        alchemyEngine = create_engine(connection_string)
        dbConnection = alchemyEngine.connect()

        data = pd.read_sql("select * from raw_table", dbConnection)
        print('data loaded from warehouse successfully')

        print('starting table organization . . .')
        data.info()
        print(f'shape: {data.shape}')
        print(f'columns: {data.columns}')

        # separating the raw data into base data and tracking data
        # base data separation
        base_df = data.iloc[0:, 0:10]
        # adding the base data to the DWH under the name base_data
        base_table_Name = 'base_table'
        base_df.to_sql(base_table_Name, dbConnection, index=False)

        # tracking data separation
        tracking_data = data.iloc[0:, 10:]
        tracking_data.insert(0, 'track_id', list(data['track_id'].values),
                             False)
        # adding the tracking data to the DWH under the name tracking_data
        tracking_data_Table_Name = 'tracking_table'
        tracking_data.to_sql(tracking_data_Table_Name, dbConnection,
                             index=False)
    except ValueError as vx:
        print(vx)
    except Exception as e:
        print(e)
    else:
        print(f"PostgreSQL tables '{tracking_data_Table_Name}' and "
              + f"'{base_table_Name}' has been created successfully.")
    finally:
        # Close the database connection
        dbConnection.close()
        return 'table organization completed'


# TODO: refactor this handler to an ETL separate script
def createTrackingDetailTable():
    """
    A detail tracking trajectory table creating handler.
    Creates a detail tracking trajectory table
    """
    print('\nstarting loading data from DWH . . .')
    try:
        # create connection to database
        alchemyEngine = create_engine(connection_string)
        dbConnection = alchemyEngine.connect()

        data = pd.read_sql("select * from raw_table", dbConnection)
        print('data loaded from warehouse successfully')

        print('starting detail tracking trajectory table creation . . .')
        data.info()
        print(f'shape: {data.shape}')
        print(f'columns: {data.columns}')

        # detail tracking trajectory data preparation
        detail_tracking_data = data.iloc[:, 4:10]
        detail_tracking_data.insert(0, 'track_id',
                                    list(data['track_id'].values), False)
        # adding the tracking data to the DWH under the name of
        # detail_tracking_data
        detail_tracking_data_Table_Name = 'detail_tracking_table'
        detail_tracking_data.to_sql(detail_tracking_data_Table_Name,
                                    dbConnection, index=False)
    except ValueError as vx:
        print(vx)
    except Exception as e:
        print(e)
    else:
        print(f"PostgreSQL table '{detail_tracking_data_Table_Name}' has "
              + "been created successfully.")
    finally:
        # Close the database connection
        dbConnection.close()
        return 'detail tracking trajectory completed'


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
# data table organizer task - fourth task
organize_tables = PythonOperator(
    task_id='organize_data_tables',
    python_callable=organizeTables,
    dag=etl_dag
)

# TODO: refactor this handler to an ETL separate script
# trajectory details table creating task - final task
create_details = PythonOperator(
    task_id='create_detailed_trajectories',
    python_callable=createTrackingDetailTable,
    dag=etl_dag
)

loadDataToDWH()
organizeTables()
createTrackingDetailTable()

# entry_point >> extract_data >> mysql1
entry_point >> extract_data >> load_data_to_postgreSQL_DWH >> organize_tables >> create_details
print('\nETL data pipeline DAG over and out')
