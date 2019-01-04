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

    **optional fields needed for copy into method**
    aws_access_key_id=
    aws_secret_access_key=

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
            account = cp.get(self.db_name, 'account'),
            user = cp.get(self.db_name, 'user'),
            password = cp.get(self.db_name, 'password'),
            host = cp.get(self.db_name, 'host'),
            database = cp.get(self.db_name, 'database'),
            port = cp.get(self.db_name, 'port')
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



    def copy_into(self, query, pprint=False):
        '''
        use copy into method to send and load data to and from S3:
            1) unload from s3 https://docs.snowflake.net/manuals/user-guide/data-unload-s3.html
            2) copy from s3 https://docs.snowflake.net/manuals/user-guide/data-load-s3.html

        
        database.conf file must have s3 credentials i.e.

        aws_access_key_id=
        aws_secret_access_key=
        
        query <string> - sql statement must include AWS credentials variables
        
        ex)
        
        COPY INTO test_schema.test_table FROM 's3://<bucket>/test_key'
        FILE_FORMAT = (
                FIELD_DELIMITER = '|' 
                COMPRESSION = gzip
                )
        CREDENTIALS = (aws_key_id='{aws_access}' aws_secret_key='{aws_secret}')


        pprint optional <boolean> -  prints formated sql query and time to execute in minutes

        '''
        conn = self.connect_to_db()
        cp = configparser.ConfigParser()
        cp.read(self.config_file)

        aws_creds = {
            'aws_access': cp.get(self.db_name, 'aws_access_key_id'),
            'aws_secret': cp.get(self.db_name, 'aws_secret_access_key')
            }

        creds = "CREDENTIALS = (aws_key_id='{aws_access}' aws_secret_key='{aws_secret}')"

        if pprint == True:
            clock = timer()
            print(self.format_sql(query))


        with conn.cursor() as cur:
            try:                
                cur.execute(query.format(**aws_creds))
                conn.commit()
            finally:
                self.close_conn()


    def get_df_from_query(self, query, params=None, pprint=False, server_cur=False):
        '''
        query <string> - sql state ment
        params optional <tuple> - user input variables to prevent sql injection
                                  sql statement should be formated with question
                                  marks where variables should be placed

                                  e.g. WHERE username = %s
        pprint optional <boolean> -  prints formated sql query and time to execute in minutes
        server_cur optional <boolean> - will return a server side cursor which you can iterate by using .fetchmany(<iterationsize>)
        
        returns a panadas dataframe
        '''
        clock = timer()
        conn = self.connect_to_db()
        
        if pprint==True:
            print(self.format_sql(query))

        if server_cur==True:
            cur = conn.cursor()
            cur.execute(query, params)
            return cur

        with conn.cursor() as cur:
            try:
                cur.execute(query, params)
                data = cur.fetchall()
                columns = [desc[0] for desc in cur.description]
                conn.commit()
            finally:
                cur.close()
                self.close_conn()

        if pprint==True:
            clock.print_lap('m')

        df = pd.DataFrame(data, columns=columns)
        return df


