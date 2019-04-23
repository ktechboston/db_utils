from db_utils.timer import timer
from db_utils import default_path
import numpy as np
import pandas as pd
import sqlparse
import configparser
import sqlparse
import os

class db_connect():
    '''
    parent python database connection class utilizing
    python database API specification v2.0
    https://www.python.org/dev/peps/pep-0249/#connection-methods
    '''
    
    def __init__(self, db_name=None, config_file=default_path):
        '''
        config_file <string> default configuration location ~/.databases.conf
        '''
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


    def connect_to_db(self):
        '''
        implemented by child classes returns a database connection
        '''
         
        database_connection = None
        self.conn = database_connection


    def format_sql(self, query):
        '''
        returns formated sql query
        '''
        return '\n' + sqlparse.format(query, reindent=True, keyword_case='upper')


    def get_df_from_query(self, query, params=None, pprint=False):
        '''
        query <string> - sql statement
        params optional <tuple> - user input variables to prevent sql injection
                                  sql statement should be formated with question
                                  marks where variables should be placed

                                  e.g. WHERE username = %s
        pprint optional <boolean> -  prints formated sql query and time to execute in minutes

        returns a panadas dataframe
        '''
        clock = timer()
        conn = self.connect_to_db()
        
        if pprint==True:
            print(self.format_sql(query))

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


    def get_arr_from_query(self, query, params=None, pprint=False):
        '''
        query <string> - sql statement
        params optional <tuple> - user input variables to prevent sql injection
                                  sql statement should be formated with question
                                  marks where variables should be placed

                                  e.g. WHERE username = %s
        pprint optional <boolean> -  prints formated sql query and time to execute in minutes

        returns a list of lists
        '''
        results_arr = []
        clock = timer()
        conn = self.connect_to_db()

        if pprint == True:
            print(self.format_sql(query))

        with conn.cursor() as cur:
            try:
                cur.execute(query, params)
                data = cur.fetchall()
                columns = [desc[0] for desc in cur.description]
                results_arr.append(columns)
                conn.commit()
            finally:
                cur.close()
                self.close_conn()

        if pprint==True:
            clock.print_lap('m')

        for row in data:
            results_arr.append(list(row))

        return results_arr


    def update_db(self, query, params=None, pprint=False):
        '''
        query <string> - sql statement
        params optional <tuple> - user input variables to prevent sql injection
                                  sql statement should be formated with question
                                  marks where variables should be placed

                                  e.g. WHERE username = %s
        pprint optional <boolean> -  prints formated sql query and time to execute in minutes

        returns effected row count
        '''
        clock = timer()
        conn = self.connect_to_db()

        if pprint == True:
            print(self.format_sql(query))

        with conn.cursor() as cur:
            try:
                cur.execute(query, params)
                row_count = cur.rowcount
                conn.commit()
            finally:
                cur.close()
                self.close_conn()
        
        if pprint==True:
            clock.print_lap('m')
        
        return row_count


    def transaction(self, queries, pprint=False):
        '''
        method for creating transcations via psycopg2
        important to use when a rollback must be called if the
        entire series of queries do not successfully complete
        
        queries - list - sql statements
        
        returns list of row counts for each query
        '''
        row_counts = []
        conn = self.connect_to_db()
        queries.insert(0, 'BEGIN;')
        
        with conn.cursor() as cur:
            try:
                for query in queries:
                    clock = timer()
                    if pprint == True:
                        print(self.format_sql(query))
                    cur.execute(query)
                    
                    if pprint == True:
                        clock.print_lap('m')
                        
                    row_counts.append(cur.rowcount)
                conn.commit()
            except Exception as e:
                print('Rolling back transacation')
                conn.rollback()
                raise Exception(str(e))
            finally:
                self.close_conn()
        
        return row_counts


    def close_conn(self):
        try:
            self.conn.close()
        except:
            print('Error {0}'.format(e))
            pass