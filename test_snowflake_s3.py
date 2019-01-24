from db_utils.snowflake_connect import snowflake_s3
import os
import io
from pprint import pprint
config_file = os.path.join(os.environ['HOME'], '.databases.conf')
#db = snowflake_s3(config_file=config_file, db_name='snowflake')

file_format = '''
TYPE = CSV
COMPRESSION = NONE
'''


with snowflake_s3(config_file=config_file, db_name='snowflake') as db:
    db.cursor('SELECT email FROM ds_prod.recommender_scores order by email limit 1000000', file_format=file_format, pprint=True)
    pprint(db.s3_queue)
    
    while True:
        key = db.fetch(contents=True)
        if key:
            for i in key:
                print(i)

        else:
    
           break
