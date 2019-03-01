from db_utils.snowflake_connect import snowflake_connect
import os
import json
import pprint

config_file = os.path.join(os.environ['HOME'], '.databases.conf')
db = snowflake_connect(config_file=config_file, db_name='snowflake_lu')


# print(db.get_df_from_query('select * from aib.deploy_comp where cam_date = %s limit 10', pprint=True, params=('2018-07-11',)))


# pprint(db.get_dicts_from_query('select * from aib.deploy_comp where cam_date = %s limit 10', pprint=True, params=('2018-07-11',)))


# db.update_db('create table if not exists ds_dev.test(col1 varchar)', pprint=True)

# db.update_db('drop table if exists ds_dev.test', pprint=True)

## UNLOAD TO S3 FROM SNOWFLAKE EXAMPLE
## https://docs.snowflake.net/manuals/user-guide/data-unload-s3.html

out = db.copy_into('''
COPY INTO ds_scratch.conversion_raw FROM 's3://aib-ml/mnaumov/amobee.csv'
        FILE_FORMAT = (
                TYPE = CSV
                skip_header=1)
        CREDENTIALS = (aws_key_id='{aws_access}' aws_secret_key='{aws_secret}')
	''', pprint=True)



out = db.copy_into('''
COPY INTO DS_STAGING.DOSMONOS_BOUNCES
FROM 's3://aib-wih-xfer/dm_to_aib/dm_db_dump/bounces-28-02-2019.txt' 
FILE_FORMAT = ( 
	TYPE = CSV FIELD_DELIMITER = ',' 
	SKIP_HEADER = 1 
	FIELD_OPTIONALLY_ENCLOSED_BY = '"' 
	EMPTY_FIELD_AS_NULL = TRUE 
	IGNORE_UTF8_ERRORS = TRUE 
	ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE 
	ENCODING = UTF8 VALIDATE_UTF8 = FALSE ) 
	ON_ERROR = CONTINUE TRUNCATECOLUMNS = TRUE 
	CREDENTIALS = (aws_key_id='{aws_access}' aws_secret_key='{aws_secret}')
''', pprint=True)




# dicts = db.get_df_from_query('''
#     WITH mr_signups AS
#         (
#           SELECT email,
#                  LAST_VALUE(city) OVER (PARTITION BY email ORDER BY timestamp_rec) AS city,
#                  LAST_VALUE(zip) OVER (PARTITION BY email ORDER BY timestamp_rec) AS zip,
#                  LAST_VALUE(state) OVER (PARTITION BY email ORDER BY timestamp_rec) AS state,
#                  LAST_VALUE(first_name) OVER (PARTITION BY email ORDER BY timestamp_rec) AS first_name,
#                  LAST_VALUE(last_name) OVER (PARTITION BY email ORDER BY timestamp_rec) AS last_name,
#                  LAST_VALUE(ip_address) OVER (PARTITION BY email ORDER BY timestamp_rec) AS ip_address,
#                  LAST_VALUE(isp) OVER (PARTITION BY email ORDER BY timestamp_rec) AS isp
#           FROM aib.signups
#           WHERE timestamp_rec > CURRENT_DATE - 30 
#           AND   source_code LIKE 'OMG'
#         ),
#         rejects AS
#         (
#           SELECT *
#           FROM aib.rejects
#           WHERE timestamp_rec > CURRENT_DATE- 180
#           and reason like 'blacklist'
#         ),
#         removals AS
#         (
#           SELECT *
#           FROM aib.removals
#           WHERE timestamp_rec > CURRENT_DATE- 180
#           AND   (source_code LIKE 'OMG' OR sub_reason LIKE 'blacklist')
#         )
#         -- select * from removals limit 100; 
#         SELECT DISTINCT 'reactivator' AS type_id,
#                'zwkGg4YqUc2nKHejUEDpnDd3nsEYKgh2' AS api_key,
#                rs.email,
#                rs.network_code||rs.offer_id AS network_offer_id,
#                recommender_score,
#                bucket,
#                su.*
#                -- rj.*, 
#                -- rm.* 
#         FROM ds_prod.recommender_scores_lists rs
#           INNER JOIN mr_signups su USING (email)
#           LEFT JOIN rejects rj on (rj.email = rs.email)
#           LEFT JOIN removals rm on (rm.email = rs.email)
#         WHERE rs.network_code LIKE 'WI'
#         AND   rs.offer_id LIKE 'omug'
#         -- AND   recommender_score > 0.9
#         and control_user like 'false'
#           AND   rj.email IS NULL
#           AND   rm.email IS NULL 
#     limit 15000''', pprint=True)
# print("found {0} records".format(len(dicts))) 


# db.transaction([
#     """
#     truncate ds_staging.sending_script 
#     """,

#     """
#     select a
#     """

#     ], pprint=True)