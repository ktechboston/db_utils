from db_utils.snowflake_connect import snowflake_connect
import os


config_file = os.path.join(os.environ['HOME'], '.databases.conf')
db = snowflake_connect(config_file)

#db.connect_to_db()
#db.update_db('create table if exists ds_dev.test(col1 varchar)', pprint=True)
row = db.update_db("""
INSERT INTO ds_dev.test
(
  col1
)
VALUES
(
  1
);
	""", pprint=True)

print(row)

print(db.get_df_from_query('select * from aib.deploy_comp limit 10', pprint=True))