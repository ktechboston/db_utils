from db_utils.DBUtil import DBUtil
from pprint import pprint
import os


config_file = os.path.join(os.environ['HOME'], '.databases.conf')
db = DBUtil(config_file=config_file, db_name='redshift')


print(db.get_df_from_query('select * from aib.deploy_comp where cam_date = %s limit 10', pprint=True, params=('2018-07-11',)))

pprint(db.get_arr_from_query('select * from aib.deploy_comp where cam_date = %s limit 10', pprint=True, params=('2018-07-11',)))


db.update_db('create table if not exists ds_dev.test(col1 varchar)', pprint=True)

db.update_db('drop table if exists ds_dev.test', pprint=True)