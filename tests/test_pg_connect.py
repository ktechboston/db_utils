import unittest

import sys
import os
from pprint import pprint
sys.path.insert(0, '..')
from db_utils.pg_connect import pg_connect
from db_utils.timer import sql_ts


config_file = 'databases.conf'
db = pg_connect('postgres', config_file)


class TestPgConnect(unittest.TestCase):


	def setUp(self):
		print ("\n--- Prepare environment ---")

	def tearDown(self):
		print ("--- Clean up ---\n")


	def test_updatedb_createTable(self):

		print("test create table function...")

		db.update_db('create table if not exists test_user(name varchar);', pprint=False)
		cr = db.update_db('select count(*) from test_user', pprint = False)
		self.assertEqual(cr, -1, 'create statement failure')


	def test_updatedb_insert(self):

		print("test insert function...")

		db.update_db('create table if not exists test_user(name varchar);', pprint=False)
		insert1 = db.update_db("insert into test_user(name) values ('Lu')", pprint=False)

		self.assertEqual(insert1, 1, 'insert statement failure')


	def test_update_db_insertParam(self):

		print("test insert function with parameters...")

		db.update_db('create table if not exists test_user(name varchar);', pprint=False)
		insert2 = db.update_db("insert into test_user(name) values (?)", params=('Terry',), pprint=False)

		self.assertEqual(insert2, 1, 'insert statement failure')


	def test_update_db_delete(self):

		print("test delete function...")

		db.update_db('create table if not exists test_user(name varchar);', pprint=False)
		insert1 = db.update_db("insert into test_user(name) values ('Lu')", pprint=False)
		# insert2 = db.update_db("insert into test_user(name) values (?)", params=('Terry',), pprint=False)
		delete = db.update_db('delete from test_user', pprint=False)

		self.assertEqual(delete, 2, 'delete statement failure')


	def test_updatedb_drop(self):

		print("test drop function...")

		db.update_db('create table if not exists test_user(name varchar);', pprint=False)
		up1 = db.update_db('drop table if exists test_user;', pprint=False)

		self.assertEqual(up1, -1, 'drop table statement returned non 0 row update')


	# def test_dicts_query(self):

	# 	print('testing get_dicts_from_query...')
	# 	TEST_ROW = 'one test row'

	# 	db.update_db('drop table if exists test_table;', pprint=False)
	# 	db.update_db('create table if not exists test_table(name varchar);', pprint=False)
	# 	db.update_db("insert into test_table(name) values (?)", params=(TEST_ROW,), pprint=False)


	# 	df = db.get_dicts_from_query('''
	# 	SELECT * FROM test_table
	# 	''', pprint=False)

	# 	pprint(df)


	def test_arr_query(self):

		print('testing get_arr_from_query method....')
		TEST_ROW = 'one test row'

		db.update_db('drop table if exists test_table;', pprint=False)
		db.update_db('create table if not exists test_table(name varchar);', pprint=False)
		db.update_db("insert into test_table(name) values %s", params=(TEST_ROW,), pprint=False)

		arr = db.get_arr_from_query('''
		SELECT * FROM test_table
		''', pprint=False)

		self.assertEqual(arr[1][0], TEST_ROW)

	
	def test_df_query(self):

		print('testing get_df_from_query method....')
		TEST_ROW = 'one test row'

		db.update_db('drop table if exists test_table;', pprint=False)
		db.update_db('create table if not exists test_table(name varchar);', pprint=False)
		db.update_db("insert into test_table(name) values (?)", params=(TEST_ROW,), pprint=False)

		df = db.get_df_from_query('''
		SELECT * FROM test_table
		''', pprint=False)

		self.assertEqual(df['name'][0], TEST_ROW)


####################################################
	def test_transaction_noError(self):
		
		print('testing transcation without error...')
		sql_list = [

		'''
		drop table if exists test_table
		''',

		'''
		CREATE TABLE IF NOT EXISTS test_table(a_col varchar(20) PRIMARY KEY)
		''',

		'''
		INSERT INTO test_table(a_col) VALUES ('first test row')
		''',

		'''
		INSERT INTO test_table(a_col) VALUES ('second test row')
		''',

		'''
		DROP TABLE test_table
		'''
		]

		expected_output = [0, 0, 1, 1, 0]
		row_update = db.transaction(sql_list, pprint=False)
		self.assertEqual(row_update, expected_output)


	def test_transaction_error(self):
		
		print("testing transcation with error...")

		sql_list = [

		'''
		drop table if exists test_table
		''',

		'''
		CREATE TABLE IF NOT EXISTS test_table(a_col varchar(20) PRIMARY KEY)
		''',

		'''
		INSERT INTO test_table(a_col) VALUES ('first test row')
		''',

		'''
		SELECT sql_sytnax_error FORM TALBE
		'''
		]

		self.assertRaises(sqlite3.ProgrammingError, db.transaction, sql_list, pprint=False)



unittest.main()

# pgtables = db.get_df_from_query('SELECT * FROM pg_tables', pprint=True)

# print(pgtables)

# # print(sql_ts('now'))

# db.update_db('drop table if exists errors', pprint=True)
# db.update_db('drop table if exists banqi;')
# print(db.get_df_from_query('select count(*) from pg_tables limit 10', pprint=True).head())


# db.csv_to_table('predictions_prediction_wh.txt', 'predictions', append=True)

# print(db.get_df_from_query('select count(*) from pg_tables limit 10', pprint=True).head())

# db.update_db('drop table if exists predictions;')

# db.csv_to_table('predictions_prediction_wh.txt', 'predictions', append=False)

# print(db.get_df_from_query('select count(*) from pg_tables limit 10', pprint=True).head())
