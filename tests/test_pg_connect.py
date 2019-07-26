import unittest
import sys
import os
import csv
from pprint import pprint
sys.path.insert(0, '..')
from db_utils.pg_connect import pg_connect



config_file = 'databases.conf'
db = pg_connect('postgres', config_file)

class test_pg_connect(unittest.TestCase):
    def setUp(self):
        db.update_db('DROP TABLE IF EXISTS test_user;', pprint=True)
        db.update_db('CREATE TABLE IF NOT EXISTS test_user(name VARCHAR);', pprint=True)


    def tearDown(self):
        db.update_db('DROP TABLE IF EXISTS test_user;', pprint=True)


    def test_updatedb(self):        
        insert1 = db.update_db('''
            INSERT INTO test_user(name) 
            VALUES ('Lu')''', pprint=True)
        self.assertEqual(insert1, 1)



        insert2 = db.update_db('''
        INSERT INTO test_user(name) VALUES (%s)
        ''', 
        params=('Terry',), 
        pprint=True)

        self.assertEqual(insert2, 1)


        delete = db.update_db('''
        DELETE FROM test_user WHERE name = 'Terry'
        ''', pprint=True)

        self.assertEqual(delete, 1)


    def test_dicts_query(self):
        val = 'test row'
        db.update_db('''
            INSERT INTO test_table(name) VALUES (%s)
        ''', params=(val,), pprint=False)

        dicts = db.get_dicts_from_query('''
        SELECT * FROM test_table
        ''', pprint=False)

        pprint(dicts)
        self.assertEqual(df[0].get('name'),val)


    def test_arr_query(self):
        val = 'test row'
        db.update_db('''
            INSERT INTO test_table(name) VALUES (%s)
            ''', 
            params=(val,), 
            pprint=True)

        arr = db.get_arr_from_query('''
        SELECT * FROM test_table
        ''', pprint=False)

        self.assertEqual(arr[1][0], val)

    
    # def test_df_query(self):

    #     print('testing get_df_from_query method....')
    #     TEST_ROW = 'one test row'

    #     db.update_db('drop table if exists test_table;', pprint=False)
    #     db.update_db('create table if not exists test_table(name varchar);', pprint=False)
    #     db.update_db("insert into test_table(name) values (%s)", params=(TEST_ROW,), pprint=False)

    #     df = db.get_df_from_query('''
    #     SELECT * FROM test_table
    #     ''', pprint=False)

    #     self.assertEqual(df['name'][0], TEST_ROW)


    # def test_transaction_noError(self):
        
    #     print('testing transcation with no error...')

    #     db.update_db('drop table if exists test_user;', pprint=False)

    #     sql_list = [

    #     '''
    #     drop table if exists test_table
    #     ''',

    #     '''
    #     CREATE TABLE IF NOT EXISTS test_table(a_col varchar(20) PRIMARY KEY)
    #     ''',

    #     '''
    #     INSERT INTO test_table(a_col) VALUES ('first test row')
    #     ''',

    #     '''
    #     INSERT INTO test_table(a_col) VALUES ('second test row')
    #     ''',

    #     '''
    #     DROP TABLE test_table
    #     '''
    #     ]

    #     expected_output = [-1, -1, -1, 1, 1, -1]
    #     row_update = db.transaction(sql_list, pprint=False)

    #     self.assertEqual(row_update, expected_output)


    # def test_transaction_error(self):
        
    #     print("testing transcation with error...")

    #     db.update_db('drop table if exists test_user;', pprint=False)

    #     sql_list = [

    #     '''
    #     drop table if exists test_table
    #     ''',

    #     '''
    #     CREATE TABLE IF NOT EXISTS test_table(a_col varchar(20) PRIMARY KEY)
    #     ''',

    #     '''
    #     INSERT INTO test_table(a_col) VALUES ('first test row')
    #     ''',

    #     '''
    #     SELECT sql_sytnax_error FORM TALBE
    #     '''
    #     ]

    #     self.assertRaises(Exception, db.transaction, sql_list, pprint=False)

    #     rows = db.update_db("select * from test_table")
    #     self.assertEqual(rows, 1)


    # def test_write_arr_to_table(self):

    #     print("testing write_array_to_table...")

    #     db.update_db('drop table if exists test_table;', pprint=False)

    #     testArray = [['v1','v2','v3','v4','v5'],['a1','a2','a3','a4','a5'],['b1','b2','b3','b4','b5'],['c1','c2','c3','c4','c5'],['d1','d2','d3','d4','d5']]
    #     tablename = 'test_table'
    #     columns = ['column1','column2','column3','column4','column5']

    #     db.write_arr_to_table(testArray,tablename, columns)

    #     row_count = db.get_arr_from_query("select count(*) from test_table")
    #     element = db.get_arr_from_query("select column1 from test_table")

    #     self.assertEqual(row_count[1], [5])
    #     self.assertEqual(element[1:], [['v1'], ['a1'], ['b1'], ['c1'], ['d1']])


    # def test_csv_to_table(self):

    #     print("testing csv_to_table...")

    #     db.update_db("drop table if exists test_table")
    #     db.csv_to_table('sample.csv', 'test_table', append=False)

    #     columnArray = None
    #     csv_row_count = None

    #     with open("sample.csv", "r") as f:

    #         columnsString = f.readline()
    #         columnsString = columnsString.replace('\n','')
    #         columnsString = columnsString.replace('_',' ')
    #         columnsString = columnsString.lower()
    #         columnArray = columnsString.split(",")

    #         reader = csv.reader(f, delimiter = ",")
    #         data = list(reader)
    #         csv_row_count = len(data)


    #     column_list = db.get_arr_from_query('''
    #         SELECT column_name
    #         FROM INFORMATION_SCHEMA.COLUMNS
    #         WHERE TABLE_NAME = 'test_table'
    #         ''')
    #     column_list = column_list[1:]
    #     result_list = []

    #     for c in column_list:
    #         result_list.append(c[0].replace('_',' '))

    #     sql_row_count = db.get_arr_from_query("select count(*) from test_table")    

    #     self.assertEqual(columnArray, result_list)
    #     self.assertEqual(csv_row_count, sql_row_count[1][0])



    # def test_copy_expert(self):

    #     print("testing copy_expert...")

    #     db.update_db("drop table if exists test_table")
    #     db.csv_to_table('sample.csv', 'test_table', append=False)


    #     # testing copy table to file
    #     print("testing copy table to file...")

    #     sql1 = """COPY test_table
    #     TO STDOUT
    #     WITH CSV HEADER"""

    #     csv_row_count1 = 0

    #     with open("test1.csv",'w+') as f:
    #         db.copy_expert(sql1, f)

    #     with open("test1.csv", "r") as f:
    #         reader = csv.reader(f, delimiter = ",")
    #         data = list(reader)
    #         csv_row_count1 = len(data)

    #     sql_row_count1 = db.get_arr_from_query("select count(*) from test_table")
        
    #     self.assertEqual(sql_row_count1[1][0]+1, csv_row_count1)



    #     # testing copy file to table
    #     print("testing copy file to table...")

    #     db.update_db("delete from test_table")

    #     sql2 = """COPY test_table
    #     FROM STDIN
    #     WITH CSV HEADER"""

    #     csv_row_count2 = 0

    #     with open("test1.csv",'r') as f:
    #         db.copy_expert(sql2, f)

    #     with open("test1.csv", "r") as f:
    #         reader = csv.reader(f, delimiter = ",")
    #         data = list(reader)
    #         csv_row_count2 = len(data)

    #     sql_row_count2 = db.get_arr_from_query("select count(*) from test_table")
        
    #     self.assertEqual(csv_row_count2, sql_row_count2[1][0]+1)


unittest.main()