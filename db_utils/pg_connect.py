from db_utils.timer import timer
from db_utils.db_connect import db_connect
import psycopg2
import psycopg2.extras
import configparser
from psycopg2.pool import SimpleConnectionPool
import numpy as np
import pandas as pd
import sqlparse


class pg_connect(db_connect):
    '''
    Database connection class to interact with Postgres or Redshift Databases

    requirements: database.conf file with the following format:

    [database_name]
    host=
    user=test_user
    password=
    port=
    database=

    ex)

    .databases.conf
    [redshift_example]
    host=redshift.example.com
    user=test_user
    password=password
    port=5439
    database=test_db

    >>> from db_utils.DBUtil import DBUtil
    >>>
    >>> db = DBUtil('redshift_example', '.databases.conf')
    >>> db.get_arr_from_query('select * from test', pprint=True)


    '''
    def connect_to_db(self):
        db_name = self.db_name
        cp = configparser.ConfigParser()
        cp.read(self.config_file)
        password = cp.get(db_name, "password")
        user = cp.get(db_name, "user")
        database = cp.get(db_name, "database")
        host = cp.get(db_name, "host")
        port = cp.get(db_name, "port")


        kwargs = {
                "host":host,
                "password":password,
                "user":user,
                "dbname":database, 
                "port":port
                }

        self.conn = psycopg2.connect(**kwargs)
        return self.conn

    def get_df_from_query(self, query, params=None, pprint=False, to_df=True, server_cur=False, itersize=20000, commit=True):
        clock = timer()
        conn = self.connect_to_db()
        
        if pprint==True:
            print(self.format_sql(query))

        if server_cur == True:
            cur = conn.cursor('server_side_cursor')
            cur.itersize = itersize
            cur.execute(query, params)            
            return cur
        else:
            with conn.cursor() as cur:
                cur.execute(query, params)
                data = cur.fetchall()
                columns = [desc[0] for desc in cur.description]
        
        if commit == True:
            conn.commit()
        
        self.close_conn()
        
        if pprint==True:
            clock.print_lap('m')

        if to_df == True: 
            df = pd.DataFrame(data, columns=columns)
            return df
        else:
            return data, columns


    def write_df_to_table(self, df, tablename, new_table=True, batched=False, pkeys={}, with_index=False):
        arr = []
        if with_index is True: 
            columns = ["index"] + list(df.columns) 
        else: 
            columns = list(df.columns ) 
        for index, row in df.iterrows():
            if with_index is True: 
                row = [str(index)] + [ str(i)[:255] for i in row.tolist()]
            else: 
                row = [ str(i)[:255] for i in row.tolist()]
            arr.append(row)
        if batched == False: self.write_arr_to_table(arr, tablename, columns, new_table)
        if batched == True: self.batch_write_arr_to_table(arr, tablename, columns, pkeys, new_table)

    def write_arr_to_table(self, arr, tablename, columns, new_table=True):

        conn = self.connect_to_db()

        column_str = "({0})".format( ",".join(columns))
        column_def = "({0} varchar(256) )".format( " varchar(256),".join(columns))
        value_str = "({0})".format( ",".join(["%s" for c in columns]))

        sql = "insert into {0} {1} values {2};".format(tablename, column_str, value_str)

        try:
            print(sql, arr[0])
        except IndexError as e:
            print(e, len(arr))

        with conn.cursor() as cur:
            if new_table==True:
                cur.execute("DROP TABLE IF EXISTS {0}".format(tablename))
                cur.execute("CREATE TABLE {0} {1}".format(tablename, column_def))
            try:
                for row in arr:
                    cur.execute(sql, row )

            except Exception as e:
                print(e)
                self.close_conn()
                raise e


        conn.commit()
        self.close_conn()
        return 0
    
    def batch_write_arr_to_table(self, arr, tablename, columns, pkeys, new_table=True):
        conn = self.connect_to_db()

        dist_str = 'distkey(' + pkeys['dist'] + ')' if 'dist' in pkeys else ''
        sort_str = 'sortkey(' + pkeys['sort'] + ')' if 'sort' in pkeys else ''
        keys = [ x for x in [dist_str,sort_str] if x != '' ]
        column_str = "({0})".format( ",".join(columns))
        column_def = '(' + (', ').join([x + ' varchar(256)' for x in columns]) + ') ' + (' ').join(keys)

        with conn.cursor() as cur:
            if new_table==True:
                cur.execute("DROP TABLE IF EXISTS {0}".format(tablename))
                cur.execute("CREATE TABLE {0} {1}".format(tablename, column_def))
                
            data = []; c = 0
            for row in arr:
                c += 1
                data.append(str(tuple(row)))
                if (len(data) == 20000) or (c == len(arr)):
                    value_str = ', '.join(data)
                    sql = "insert into {0} {1} values {2};".format(tablename, column_str, value_str)
                    try: cur.execute(sql)
                    except Exception as e:
                        print(e); self.close_conn()
                        raise e
                    data = []

        conn.commit()
        self.close_conn()
        return 0


    def copy_expert(self, sql, file, pprint=False):
        '''
        examples): 
        copying data from a flat file to a table:

        >>sql = """COPY test_table
        FROM STDIN
        WITH (FORMAT csv)"""
        
        >>with open('file.csv', 'r') as fl:
        >>    copy_expert(sql, fl)
        

        copying date from a table to a flat file:


        >>sql = """COPY test_table
        TO STDOUT
        WITH (FORMAT csv)"""
        
        >>with open('file.csv', 'w') as fl:
        >>    copy_expert(sql, fl)

        Postgres copy syntax:
        https://www.postgresql.org/docs/9.6/sql-copy.html
        '''

        conn = self.connect_to_db()
        if pprint == True:
            clock = timer()
            print(self.format_sql(sql))
        conn.cursor().copy_expert(sql, file)
        conn.commit()

        if pprint == True:
            clock.print_lap('m')


    def get_dicts_from_query(self, query, params=None, pprint=False):
        conn = self.connect_to_db()

        if pprint == True:
            clock = timer()
            print(self.format_sql(query))
        
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            try:                
                cur.execute(query, params)
                conn.commit()
                data = cur.fetchall()

            finally:
                self.close_conn()
            
            

            return data
    

