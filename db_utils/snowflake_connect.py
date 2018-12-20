from db_utils.timer import timer
from db_utils.db_connect import db_connect
from snowflake.connector import DictCursor
import snowflake.connector
import numpy as np
import pandas as pd
import sqlparse
import configparser


class snowflake_connect(db_connect):
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

        self.conn = conn
        return conn


    def get_dicts_from_query(self, query, params=None, pprint=False):
        '''
        query <string> - sql statement
        params optional <tuple> - user input variables to prevent sql injection
                                  sql statement should be formated with question
                                  marks where variables should be placed

                                  e.g. WHERE username = %s
        pprint optional <boolean> -  prints formated sql query and time to execute in minutes

        returns a list of dictionaries
        '''
        conn = self.connect_to_db()

        if pprint == True:
            clock = timer()
            print(self.format_sql(query))
        
        with conn.cursor(DictCursor) as cur:
            try:                
                cur.execute(query, params)
                conn.commit()
            finally:
                self.close_conn()
            
            

            return cur.fetchall()