# import libraries
import pandas as pd
from sqlalchemy import create_engine


def createEngine(local: bool = False):
    # create an engine to connect to our data base server
    if local:
        path = 'mysql+pymysql://root:@localhost/airflow_db'
    else:
        path = 'https://drive.google.com/file/d/1LdE5yAzEeNWC7xVAQ6tv4A-p0sS58pJi/view?usp=sharing'
    engine = create_engine(path)
    return engine


def addToTable(engine, all: bool = False):
    # writing to the data base server
    if all:
        try:
            print('reading csv files as a pandas dataframe...')
            user_ove_df = pd.read_csv('20181024_d1_0830_0900.csv')

            print('writing to the database...')
            frame1 = user_ove_df.sample(frac=0.01).to_sql("test_table",
                                                          con=engine,
                                                          if_exists='replace')

            print('table successfully saved to database')
        except Exception as e:
            print("Error writing to database: ", e)
    else:
        try:
            print('reading csv file as a pandas dataframe...')
            user_ove_df = pd.read_csv('20181024_d1_0830_0900.csv')

            print('writing to the database...')
            frame = user_ove_df.sample(frac=0.01).to_sql("test_table",
                                                         con=engine,
                                                         if_exists='replace')
            print('data successfully saved to database')
        except Exception as e:
            print("Error writing to database: ", e)


if __name__ == "__main__":

    # get database engine
    print('generating database engine...')
    engine = createEngine()

    addToTable(engine, all=False)

    # reading data from the data base server
    fromDb = pd.read_sql("select * from telecom.test_table", engine)
    print(fromDb.info())
