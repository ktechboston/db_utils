from db_utils.timer import timer
from db_utils.db_connect import db_connect
from db_utils.s3_connect import s3_connect, snowflake_dmpky
from snowflake.connector import DictCursor
from pprint import pprint as pretty_print
from jinja2 import Template
import snowflake.connector
import numpy as np
import pandas as pd
import sqlparse
import configparser
import uuid
import io
import warnings


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
                data = cur.fetchall()
            finally:
                self.close_conn()
            
            return data


    def copy_into(self, query, pprint=False):
        '''
        use copy into method to send and load data to and from S3:
            https://docs.snowflake.net/manuals/sql-reference/sql/copy-into-table.html
            1) unload to s3
            2) copy from s3

        
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

        returns dictionary of metadata
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


        with conn.cursor(DictCursor) as cur:
            try:                
                cur.execute(query.format(**aws_creds))
                data = cur.fetchall()
                conn.commit()
                if pprint == True:
                    clock.print_lap('m')
                    pretty_print(data)

                status = data[0].get('status')
                if status=='LOAD_FAILED':
                    raise snowflake.connector.errors.ProgrammingError('{}'.format(data[0]))
                
                elif status=='PARTIALLY_LOADED':
                    warnings.warn('partially load - {0}'.format(data[0].get('first_error')))

            
            finally:
                self.close_conn()

        return data


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




class snowflake_s3(snowflake_connect):

    def __init__(self, db_name=None, config_file=None):
        if db_name is None: 
            raise Exception("Please provide db_name=the-section-in-your-config-file as an argument") 
        if config_file is None: 
            raise Exception("Please provide config_file=/path/to/your/file as an argument") 
        try: 
            open(config_file)
        except: 
            raise Exception("Cannot open config_file. Please check the path to your config files") 

        self.db_name = db_name
        self.config_file = config_file
        self.s3_queue = []
        self.bucket = None
        self.s3_prefix = None
        self.s3conn = s3_connect(self.config_file, self.db_name)


    def __enter__(self):
        '''
        context manager
        '''
        return self


    def get_aws_creds(self):
        creds = configparser.ConfigParser()
        creds.read(self.config_file)
        self.access_key = creds.get(self.db_name, 'aws_access_key_id')
        self.secret_key = creds.get(self.db_name, 'aws_secret_access_key')
        self.default_bucket = creds.get(self.db_name, 'default_bucket')


    def cursor(self, query, file_format=None, bucket=None, pprint=False):
        '''
        dumps large dataset to s3

        .database.conf file must have s3 credentials i.e.
            aws_access_key_id=
            aws_secret_access_key=
            default_bucket=

        query <string> - sql statement

        file_format <string> - snowflake unload formatTypeOptions 
                                https://docs.snowflake.net/manuals/sql-reference/sql/copy-into-table.html

        bucket <string> - s3 bucket if not default_bucket

        pprint <boolean> - prints formatted sql for s3 dump

        returns number of files dumped to s3
        use fetch method to retrieve s3 files one at a time
        '''

        self.get_aws_creds()
        self.s3_prefix = 'db_utils/' + str(uuid.uuid4())

        if bucket == None:
            self.bucket = self.default_bucket

        s3 = 's3://{0}/{1}'.format(self.bucket, self.s3_prefix)
        
        dump_vars = {
            's3': s3,
            'file_format': file_format,
            'query': query
        }

        s3_dump = Template('''
        COPY INTO '{{ s3 }}' FROM ({{ query }}){% if file_format %}
        FILE_FORMAT = (
            {{ file_format }}
            )
        {% endif %}
        CREDENTIALS = (aws_key_id='{aws_access}' aws_secret_key='{aws_secret}')
        OVERWRITE = FALSE
        SINGLE = FALSE''')

        self.copy_into(s3_dump.render(dump_vars), pprint=pprint)
        keys = self.s3conn.list_keys(prefix=self.s3_prefix)
        s3_keys = [snowflake_dmpky(key) for key in keys]
        s3_keys.sort(reverse=True)

        self.s3_queue = s3_keys

        return len(keys)


    def fetch(self, download=False, gz=True):
        '''
        download <boolean> - if set to True downloads the s3 chunk to your current working directory
        
        gz <boolean> - by default s3 chunks are gzipped, if data explicitly not compressed set to False

        returns file pointer or stringIO stream from s3 file, must run cursor method first
        '''

        try:
            key = str(self.s3_queue.pop())
        except IndexError:
            return None


        if download==True:
            try:
                output = self.s3conn.download_file(key, bucket=self.bucket)
            except Exception as e:
                self.close()
                raise Exception(str(e))

        else:
            try:
                output = self.s3conn.get_contents(key, bucket=self.bucket, gz=gz)
            except Exception as e:
                self.close()
                raise Exception(str(e))


        self.s3conn.del_key(key, bucket=self.bucket)
        return output


    def close(self):
        '''
        cleans up s3 queue
        '''

        if self.s3_queue:
            for i in self.s3_queue:
                self.s3conn.del_key(str(i), bucket=self.bucket)

        if self.s3_prefix:
            self.s3conn.del_key(self.s3_prefix)


    def __exit__(self, exc_type, exc_value, traceback):
        self.close()




