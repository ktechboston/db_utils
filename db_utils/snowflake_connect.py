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
    account='abc123.us-east-1',
    host='abc123.us-east-1.snowflakecomputing.com'
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


