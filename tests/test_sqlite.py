import unittest
import sys
import os
import sqlite3
sys.path.insert(0, '..')
from db_utils.sqlite_connect import sqlite_connect
from contextlib import contextmanager

db = sqlite_connect('test.db')


class test_sqlite_connect(unittest.TestCase):
    def setUp(self):
        db.update_db('DROP TABLE IF EXISTS test_table', params=())
        db.update_db('CREATE TABLE IF NOT EXISTS test_table(name varchar);', params=())


    def tearDown(self):
        db.update_db('DROP TABLE IF EXISTS test_table;')


    def test_updatedb(self):        
        insert1 = db.update_db('''
            INSERT INTO test_table(name) 
            VALUES ('Mahe Drysdale')''', pprint=True)
        self.assertEqual(insert1, 1)

        insert2 = db.update_db('''
        INSERT INTO test_table(name) VALUES (?)
        ''', 
        params=('Ondrej Synek',), 
        pprint=True)

        self.assertEqual(insert2, 1)


        delete = db.update_db('''
        DELETE FROM test_table WHERE name = 'Ondrej Synek'
        ''', pprint=True)

        self.assertEqual(delete, 1)


    def test_get_arr_from_query(self):
        test_row = 'one test row'
        db.update_db(
        '''INSERT INTO test_table(name) VALUES (?)''', 
        params=(test_row,), 
        pprint=True)

        arr = db.get_arr_from_query('''
        SELECT * FROM test_table
        ''', pprint=True)

        print(arr)
        self.assertEqual(arr[1][0], test_row)

    
    def test_get_df_from_query(self):
        test_row = 'one test row'
        db.update_db(
            'INSERT INTO test_table(name) VALUES (?)', 
            params=(test_row,), 
            pprint=True)

        df = db.get_df_from_query('''
        SELECT * FROM test_table
        ''', pprint=True)

        self.assertEqual(df['name'][0], test_row)


    def test_transaction(self):
        sql_list = [

        '''
        DROP TABLE IF EXISTS transcation_table
        ''',

        '''
        CREATE TABLE IF NOT EXISTS transcation_table(a_col varchar(20) PRIMARY KEY)
        ''',

        '''
        INSERT INTO transcation_table(a_col) VALUES ('first test row')
        ''',

        '''
        INSERT INTO transcation_table(a_col) VALUES ('second test row')
        ''',

        '''
        DROP TABLE transcation_table
        '''
        ]

        expected_output = [-1, -1, -1, 1, 1, -1]
        row_update = db.transaction(sql_list, pprint=True)
        self.assertEqual(row_update, expected_output)


    def test_transaction_error(self):
        
        print("testing transcation with error...")

        sql_list = [

        '''
        DROP table if exists tans_table
        ''',

        '''
        CREATE TABLE IF NOT EXISTS tans_table(a_col varchar(20) PRIMARY KEY)
        ''',

        '''
        INSERT INTO tans_table(a_col) VALUES ('first test row')
        ''',

        '''
        SELECT sql_sytnax_error FORM TALBE
        '''
        ]

        self.assertRaises(Exception, db.transaction, sql_list, pprint=True)


unittest.main()