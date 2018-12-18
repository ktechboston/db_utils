from db_utils.timer import timer
import snowflake.connector
import numpy as np
import pandas as pd
import sqlparse
import configparser


class snowflake_connect():
    '''
    Database connection class to interact with Snowflake

    requirements: database.conf file with the following format:

    [database_name]
    account=
    host=
    user=test_user
    password=
    port=
    database=

    ex)

    .databases.conf
    [snowflake]
    account=abc123.us-east-1
    host=abc123.us-east-1.snowflakecomputing.com
    user=test_user
    password=password
    port=443
    database=test_db

    '''

    def __init__(self, config_file=None, db_name='snowflake'):
        if config_file is None: 
            raise Exception("Please provide config_file=/path/to/your/file as an argument") 
        try: 
            open(config_file)
        except: 
            raise Exception("Cannot open config_file. Please check the path to your config file")

        self.db_name = db_name
        self.config_file = config_file


    def connect_to_db(self):
        db_name = self.db_name
        cp = configparser.ConfigParser()
        cp.read(self.config_file)
        
        conn = snowflake.connector.connect(
            account = cp.get(db_name, 'account'),
            user = cp.get(db_name, 'user'),
            password = cp.get(db_name, 'password'),
            host = cp.get(db_name, 'host'),
            database = cp.get(db_name, 'database'),
            port = cp.get(db_name, 'port')
            )

        return conn


    def format_sql(self, query):
        return '\n' + sqlparse.format(query, reindent=True, keyword_case='upper')


    def update_db(self, query, params=None, pprint=False):
        clock = timer()
        conn = self.connect_to_db()

        try:
            if pprint == True:
                print(self.format_sql(query))
            
            cur = conn.cursor()
            cur.execute(query, params)
            row_count = cur.rowcount
        finally:
            conn.close()

        if pprint == True:
            clock.print_lap('m')

        return row_count


    def get_df_from_query(self, query, params=None, pprint=False, to_df=True):
        clock = timer()
        conn = self.connect_to_db()
  
        
        if pprint==True:
            print(self.format_sql(query))


        with conn.cursor() as cur:
            cur.execute(query, params)
            data = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
        
        
        if pprint==True:
            clock.print_lap('m')

        if to_df == True: 
            df = pd.DataFrame(data, columns=columns)
            return df
        else:
            return data, columns