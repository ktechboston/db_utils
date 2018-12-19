from db_utils.snowflake_connect import snowflake_connect
import os
from pprint import pprint


config_file = os.path.join(os.environ['HOME'], '.databases.conf')
db = snowflake_connect(config_file=config_file, db_name='snowflake')

#db.connect_to_db()
#db.update_db('create table if exists ds_dev.test(col1 varchar)', pprint=True)
# row = db.update_db("""
# INSERT INTO ds_dev.test
# (
#   col1
# )
# VALUES
# (
#   1
# );
# 	""", pprint=True)

# print(row)

print(db.get_df_from_query('select * from aib.deploy_comp where cam_date = %s limit 10', pprint=True, params=('2018-07-11',)))


pprint(db.get_arr_from_query('select * from aib.deploy_comp where cam_date = %s limit 10', pprint=True, params=('2018-07-11',)))

# help(snowflake_connect)