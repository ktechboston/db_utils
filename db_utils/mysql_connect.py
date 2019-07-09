# from db_utils import *
from db_utils.timer import timer
from db_utils.db_connect import db_connect
import configparser
import mysql.connector
import numpy as np
import pandas as pd
import sqlparse


###TO DO
#implement custom mysql custom cursor
#add connection pooling

class custom_cur(mysql.connector.cursor.CursorBase):
#https://github.com/mysql/mysql-connector-python/blob/master/lib/mysql/connector/connection.py#L861
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


	# def get_dicts_from_query(self, query, params=None, pprint=False):



	def get_arr_from_query(self, query, params=None, pprint=False):

		results_arr = []
		clock = timer()
		conn = self.connect_to_db()

		if pprint == True:
			print(self.format_sql(query))

		# with conn.cursor() as cur:
		cur = conn.cursor()
		
		if params:
			cur.execute(query, params)
		else:
			cur.execute(query)

		data = cur.fetchall()
		columns = [desc[0] for desc in cur.description]
		results_arr.append(columns)
		conn.commit()

		cur.close()

		if pprint==True:
			clock.print_lap('m')

		for row in data:
			results_arr.append(list(row))

		return results_arr



	def get_df_from_query(self, query, params=None, pprint=False, to_df=True, server_cur=False, itersize=1000, commit=True):
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


	def transaction(self, queries, pprint=False):
		'''
		method for creating transcations
		important to use when a rollback must be called if the
		entire series of queries do not successfully complete
		
		queries - list - sql statements
		
		returns list of row counts for each query
		'''
		row_counts = []
		conn = self.connect_to_db()
		queries.insert(0, 'BEGIN;')
		
		cur = conn.cursor()
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
			raise mysql.connector.errors.ProgrammingError(str(e))
		finally:
			self.close_conn()
		
		return row_counts

		