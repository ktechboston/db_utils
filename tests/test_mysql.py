import unittest
import os
import sys
import mysql.connector
sys.path.insert(0, '..')
from db_utils.mysql_connect import mysql_connect



db = mysql_connect(config_file='databases.conf', db_name='mysql')


class TestMySqlConnect(unittest.TestCase):

    # test updatedb
        # create table
        # insert
        # drop table

    # test dfquery
        # right dataframe?

    # test transaction
        # right
        # wrong

    def setUp(self):
        print ("\n--- Prepare environment ---")

    def tearDown(self):
        print ("--- Clean up ---\n")
    
    def test_updatedb_drop(self):

        print("test drop function...")

        up1 = db.update_db('''
        DROP TABLE IF EXISTS test_table;
        ''', pprint=False)

        self.assertEqual(up1, 0, 'drop table statement returned non 0 row update')


    def test_updatedb_createTable(self):

        print("test create table function...")

        up2 = db.update_db('''
        CREATE TABLE IF NOT EXISTS test_table(a_col varchar(20) PRIMARY KEY);
        ''', pprint=False)

        self.assertEqual(up2, 0, 'create table statement returned non 0 row update')


    def test_updatedb_insert(self):

        print("test insert function...")

        db.update_db('''
        CREATE TABLE IF NOT EXISTS test_table(a_col varchar(20) PRIMARY KEY);
        ''', pprint=False)

        in1 = db.update_db('''
        INSERT INTO test_table(a_col) VALUES ('one test row')
        ''', pprint=False)

        self.assertEqual(in1, 1, 'insert statement failure')

        dr = db.update_db('''
        DROP TABLE test_table
        ''', pprint=False)

    
    def test_dfquery(self):

        print('testing get_df_from_query method....')
        TEST_ROW = 'one test row'

        up1 = db.update_db('''
        DROP TABLE IF EXISTS test_table;
        ''', pprint=False)

        up2 = db.update_db('''
        CREATE TABLE IF NOT EXISTS test_table(a_col varchar(20) PRIMARY KEY);
        ''', pprint=False)

        in1 = db.update_db('''
        INSERT INTO test_table(a_col) VALUES (%s)
        ''', params=(TEST_ROW,), pprint=False)

        df = db.get_df_from_query('''
        SELECT * FROM test_table
        ''', pprint=False)

        self.assertEqual(df['a_col'][0], TEST_ROW)


    def test_transaction_noError(self):
        
        print('testing transcation without error...')
        sql_list = [
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


#井#井#井#井#井#井#井#井#井#井#井#井#井#井#井#井#井#井#井#井#井#井#井#井#井#井
    def test_transaction_error(self):
        
        print("testing transcation with error...")

        sql_list = [
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

        # output = db.transaction(sql_list, pprint=True)
        # self.assertRaises(mysql.connector.errors.ProgrammingError, db.transaction, 'hmmmmmmmmmmmmm...?')

        # row_count = db.update_db('SELECT * FROM test_table', pprint=True)
        # self.assertEqual(row_count, 0, 'hmm...')

        with self.assertRaises(mysql.connector.errors.ProgrammingError) as e: 
            output = db.transaction(sql_list, pprint=True)

        # row_count = db.update_db('SELECT COUNT(*) FROM test_table', pprint=True)
        # self.assertEqual(row_count, 0, 'hmm...')
            # db.get_df_from_query('SELECT * FROM test_table', pprint=False)



unittest.main()