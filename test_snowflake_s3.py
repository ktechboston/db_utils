from db_utils.snowflake_connect import snowflake_s3
import os
import io

config_file = os.path.join(os.environ['HOME'], '.databases.conf')
#db = snowflake_s3(config_file=config_file, db_name='snowflake')

file_format = '''
TYPE = CSV
COMPRESSION = NONE
'''


with snowflake_s3(config_file=config_file, db_name='snowflake') as db:
    rows = db.cursor('SELECT * FROM aib.deploy_comp limit 100000', file_format=file_format, pprint=True)
    print(db.s3_queue)
    print(rows)

    while True:
        key = db.fetch(contents=True)
        if key:
            for i in key:
                print(i)

        else:
            break
