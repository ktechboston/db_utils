from db_utils.db_connect import db_connect
from pprint import pprint
from db_utils.timer import timer
import pyodbc
import configparser
import pandas as pd


class sql_server_connect(db_connect):
    '''
    Database connection class to interact with microsoft sql server

    requirements: database.conf file with the following format:

    [sql_server]
    driver=ODBC Driver 17 for SQL Server
    server=127.0.0.1
    user=bill
    password=gates
    database=master

    '''
    def connect_to_db(self):
        db_name = self.db_name

        conn = pyodbc.connect(
            '''
            DRIVER={driver};
            SERVER={server};
            UID={user};             
            PWD={password};
            DATABASE={database};
            '''.format(**self.config_dict))

        return conn


    def get_arr_from_query(self, query, params=None, pprint=False):
        '''
        query <string> - sql statement
        params optional <tuple> - user input variables to prevent sql injection
                                  sql statement should be formated with question
                                  marks where variables should be placed

                                  e.g. WHERE username = ?
        pprint optional <boolean> -  prints formated sql query and time to execute in minutes

        returns a list of lists
        '''
        results_arr = []
        clock = timer()
        conn = self.connect_to_db()

        if pprint == True:
            print(self.format_sql(query))

        with conn.cursor() as cur:
            if params:
                cur.execute(query, params)
            else:
                cur.execute(query)

            data = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            results_arr.append(columns)
            conn.commit()

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

                                  e.g. WHERE username = ?
        pprint optional <boolean> -  prints formated sql query and time to execute in minutes

        returns effected row count
        '''
        clock = timer()
        conn = self.connect_to_db()

        if pprint == True:
            print(self.format_sql(query))

        with conn.cursor() as cur:
            if params:
                cur.execute(query, params)
            else:
                cur.execute(query)

            row_count = cur.rowcount
            conn.commit()

        
        if pprint==True:
            clock.print_lap('m')
        
        return row_count


    def get_df_from_query(self, query, params=None, pprint=False):
        '''
        query <string> - sql statement
        params optional <tuple> - user input variables to prevent sql injection
                                  sql statement should be formated with question
                                  marks where variables should be placed

                                  e.g. WHERE username = ?
        pprint optional <boolean> -  prints formated sql query and time to execute in minutes

        returns a panadas dataframe
        '''
        clock = timer()
        conn = self.connect_to_db()
        
        if pprint==True:
            print(self.format_sql(query))

        with conn.cursor() as cur:
            if params:
                cur.execute(query, params)
            else:
                cur.execute(query)
            data = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            conn.commit()


        if pprint==True:
            clock.print_lap('m')

        df = pd.DataFrame.from_records(data, columns=columns)
        return df


    def transaction(self, queries, pprint=False):
        out = 'transactions are not supported by sql_server_connect yet'
        print(out)
        return out

