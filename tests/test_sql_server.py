import os
import sys
sys.path.insert(0, '..')
from db_utils.sql_server_connect import sql_server_connect
from pprint import pprint


db = sql_server_connect('sql_server', 'databases.conf')

print(db.config_dict)
out = db.get_arr_from_query('''
SELECT
  *
FROM
  SYSOBJECTS
WHERE
  xtype = 'U';
''', pprint=True
	)

print(out)

db.update_db('drop table persons', pprint=True)

db.update_db('''
CREATE TABLE Persons (
    PersonID int,
    LastName varchar(255),
    FirstName varchar(255),
    Address varchar(255),
    City varchar(255) 
);
'''
	, pprint=True)


out = db.get_df_from_query('''
SELECT
  *
FROM
  SYSOBJECTS
WHERE
  xtype = 'U';
''', pprint=True
	)

pprint(out)
