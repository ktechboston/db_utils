from db_utils.db_connect import db_connect
from db_utils.timer import timer
import numpy as np
import pandas as pd
import sqlite3


class sqlite_connect(db_connect):
    def __init__(self, db_path):
        '''
        db_path <string> -  supply path to database file, if it doesn't exist 
                            a new one will be created.  

        ex)
        from db_utils.sqlite_connect import sqlite_connect

        db = sqlite_connect('test.db') 
        '''
        self.db = db_path


    def connect_to_db(self):
        self.conn = sqlite3.connect(self.db)
        return self.conn


    def update_db(self, query, params=(), pprint=False):
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

        cur = conn.cursor()
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


    def get_df_from_query(self, query, params=(), pprint=False):
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

        cur = conn.cursor()
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