from db_utils.timer import timer
from db_utils.db_connect import db_connect
import configparser
import mysql.connector
import numpy as np
import pandas as pd
import sqlparse


###TO DO
#implement custom mysql custom cursor

class custom_cur(mysql.connector.cursor.CursorBase):
https://github.com/mysql/mysql-connector-python/blob/master/lib/mysql/connector/connection.py#L861
	def __exit__():
		pass


#documentation link:
#https://dev.mysql.com/doc/connector-python/en/connector-python-reference.html
class mysql_connect(db_connect):

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
	            "database":database, 
	            "port":port
	            }

	    self.conn = mysql.connector.connect(**kwargs)
	    return self.conn



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