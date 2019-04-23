from db_utils.mysql_connect import mysql_connect
import os




config_file = os.path.join(os.environ['HOME'], '.databases.conf')

db = mysql_connect(config_file=config_file, db_name='mysql')

a = db.update_db('create if table does not exist test(a_col varchar(20) primary key);', pprint=True)


print(a)

