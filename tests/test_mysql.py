import unittest
import os
import sys
import mysql.connector
sys.path.insert(0, '..')
from db_utils.mysql_connect import mysql_connect


db = mysql_connect(config_file='databases.conf', db_name='mysql')


class test_mysqlconnect(unittest.TestCase):
    
    def test_updatedb(self):
        print('testing update_db method....')

        up1 = db.update_db('''
        DROP TABLE IF EXISTS test_table;
        ''', pprint=True)

        up2 = db.update_db('''
        CREATE TABLE IF NOT EXISTS test_table(a_col varchar(20) PRIMARY KEY);
        ''', pprint=True)

        in1 = db.update_db('''
        INSERT INTO test_table(a_col) VALUES ('one test row')
        ''', pprint=True)

        dr = db.update_db('''
        DROP TABLE test_table
        ''', pprint=True)

        self.assertEqual(up1, 0, 'drop table statement returned non 0 row update')
        self.assertEqual(up2, 0, 'create table statement returned non 0 row update')
        self.assertEqual(in1, 1, 'insert statement failure')
        self.assertEqual(dr, 0, 'truncate statement failure')
    
    
    def test_dfquery(self):
        print('testing get_df_from_query method....')
        TEST_ROW = 'one test row'

        up1 = db.update_db('''
        DROP TABLE IF EXISTS test_table;
        ''', pprint=True)

        up2 = db.update_db('''
        CREATE TABLE IF NOT EXISTS test_table(a_col varchar(20) PRIMARY KEY);
        ''', pprint=True)

        in1 = db.update_db('''
        INSERT INTO test_table(a_col) VALUES (%s)
        ''', params=(TEST_ROW,), pprint=True)

        df = db.get_df_from_query('''
        SELECT * FROM test_table
        ''', pprint=True)

        dr = db.update_db('''
        DROP TABLE test_table
        ''', pprint=True)

        self.assertEqual(df['a_col'][0], TEST_ROW)


    def test_transaction(self):
        print('testing transcation...')
        sql_list_1 = [
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

        expected_output_1 = [0, 0, 1, 1, 0]
        row_update = db.transaction(sql_list_1, pprint=True)
        self.assertEqual(row_update, expected_output_1)

        sql_list_2 = [
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

        with self.assertRaises(mysql.connector.errors.ProgrammingError) as e:
            db.transaction(sql_list_2, pprint=True)
            db.get_df_from_query('SELECT * FROM test_table', pprint=True)



unittest.main()