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

trun = db.update_db('''
	TRUNCATE test;
	''', pprint=True)

print(trun)