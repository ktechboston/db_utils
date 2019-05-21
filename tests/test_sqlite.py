from pprint import pprint
import sys
import os
sys.path.insert(0, '..')
from db_utils.sqlite_connect import sqlite_connect


db = sqlite_connect('test.db')

a = db.update_db('create table if not exists test_user(name varchar);', pprint=True)

db.update_db('delete from test_user', pprint=True)

db.update_db("insert into test_user(name) values ('Lu')", pprint=True)

db.update_db("insert into test_user(name) values ('Joe')", pprint=True)

db.update_db("insert into test_user(name) values (?)", params=('Terry',), pprint=True)

db.update_db("insert into test_user(name) values (?)", params=('Zach',), pprint=True)

a = db.get_df_from_query('select * from test_user', pprint=True)

print(a)