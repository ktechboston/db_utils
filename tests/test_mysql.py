import os
import sys
sys.path.insert(0, '..')
from db_utils.mysql_connect import mysql_connect



db = mysql_connect(config_file='databases.conf', db_name='mysql')


db.update_db('''
	CREATE TABLE IF NOT EXISTS test(a_col varchar(20) PRIMARY KEY);
''', pprint=True)


rows = db.update_db('''
	INSERT INTO test (a_col) values ('hello world');
	''', pprint=True)

print(rows)

print(db.get_df_from_query('SELECT * FROM test;', pprint=True))

trun = db.update_db('''
	TRUNCATE test;
	''', pprint=True)

print(trun)


queries = [
'''
INSERT INTO test (a_col) values ('testing is good');
''',

'''
INSERT INTO test (a_col) values ('unit tests would be better though');
''',


'''
INSERT INTO test (a_col) err ('heck....');
'''
]

try:
	db.transaction(queries, pprint=True)
except Exception as e:
	print(str(e))

print(db.get_df_from_query('SELECT * FROM test;', pprint=True))