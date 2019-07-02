import unittest

from pprint import pprint
import sys
import os
sys.path.insert(0, '..')
from db_utils.sqlite_connect import sqlite_connect


db = sqlite_connect('test.db')


class TestSqlite(unittest.TestCase):


	@classmethod
	def setUpClass(cls):
		print ("This setUpClass() method only called once.")

	@classmethod
	def tearDownClass(cls):
		print ("This tearDownClass() method only called once too.")


	def test_update_db_create(self):
		
		db.update_db('create table if not exists test_user(name varchar);', pprint=True)
		cr = db.update_db('select count(*) from test_user', pprint = True)
		self.assertEqual(cr, 0, 'create statement failure')

	def test_update_db_insert(self):

		insert1 = db.update_db("insert into test_user(name) values ('Lu')", pprint=True)
		self.assertEqual(insert1, 1, 'insert statement failure')

	def test_update_db_insertParam(self):

		insert2 = db.update_db("insert into test_user(name) values (?)", params=('Terry',), pprint=True)
		self.assertEqual(insert2, 1, 'insert statement failure')

	def test_update_db_delete(self):

		delete = db.update_db('delete from test_user', pprint=True)
		self.assertEqual(delete, 2, 'delete statement failure')


unittest.main()

print(a)