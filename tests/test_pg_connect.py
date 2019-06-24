
import sys
import os
from pprint import pprint
sys.path.insert(0, '..')
from db_utils.pg_connect import pg_connect
from db_utils.timer import sql_ts


config_file = 'databases.conf'
db = pg_connect('postgres', config_file)

# pgtables = db.get_df_from_query('SELECT * FROM pg_tables', pprint=True)

# print(pgtables)


# print(sql_ts('now'))

db.update_db('drop table if exists errors', pprint=True)
db.update_db('drop table if exists banqi;')
print(db.get_df_from_query('select count(*) from predictions limit 10', pprint=True).head())


db.csv_to_table('predictions_prediction_wh.txt', 'predictions', append=True)

print(db.get_df_from_query('select count(*) from predictions limit 10', pprint=True).head())

db.update_db('drop table if exists predictions;')

db.csv_to_table('predictions_prediction_wh.txt', 'predictions', append=False)

print(db.get_df_from_query('select count(*) from predictions limit 10', pprint=True).head())
