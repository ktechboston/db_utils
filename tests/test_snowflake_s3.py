from db_utils.snowflake_connect import snowflake_s3
from db_utils.timer import timer
import os
import io
from pprint import pprint
config_file = os.path.join(os.environ['HOME'], '.databases.conf')
#db = snowflake_s3(config_file=config_file, db_name='snowflake')

file_format = '''
TYPE = CSV
'''


with snowflake_s3(config_file=config_file, db_name='snowflake') as db:
    db.cursor('''
        SELECT 
            email, 
            offer_id || '~' || network_code AS offer, 
            date,
            click AS reactivation,
            send,
            sends_global,
            open_exists,
            click_exists,
            click_exists_global,
            signup_exists,
            trvs_exists,
            is_gmail,
            is_yahoo,
            is_aol,
            email_year_numbers / 2100.0 AS email_year_numbers,
            wih_exists,
            wih_dob_adj / 2100.0 AS wih_dob_adj
        FROM
        (
            SELECT 
                *, 
                sum(send) over(PARTITION BY email, date) AS sends_global,
                (CASE
                    WHEN email_numbers IS NULL THEN 0
                    WHEN email_numbers = '' THEN 0
                    WHEN length(email_numbers) = 4 and email_numbers::int < 2020 then email_numbers::int
                    WHEN length(email_numbers) = 2 and email_numbers::int < 20 then 2000 + email_numbers::int
                    WHEN length(email_numbers) = 2 and email_numbers::int >= 20 then 1900 + email_numbers::int
                    ELSE 0
                END) as email_year_numbers
            FROM
            (
                SELECT 
                    s.email, 
                    s.offer_id,
                    s.network_code,
                    s.cam_date AS date,
                    max(s.click) AS click,
                    (CASE WHEN s.email LIKE '%@gmail.com' THEN 1 ELSE 0 END) AS is_gmail,
                    (CASE WHEN s.email LIKE '%@yahoo.com' THEN 1 ELSE 0 END) AS is_yahoo,
                    (CASE WHEN s.email LIKE '%@aol.com' THEN 1 ELSE 0 END) AS is_aol,
                    left(regexp_replace(s.email, '[^0-9]', ''), 8) AS email_numbers,
                    1.0::float AS send,
                    max(s.open_exists) AS open_exists,
                    max(s.click_exists) AS click_exists,
                    max(s.click_exists_global) AS click_exists_global,
                    max(s.signup_exists) AS signup_exists,
                    max(CASE WHEN ned.traverse_signal = 1 THEN 1 ELSE 0 END) AS trvs_exists,
                    max(CASE WHEN ned.wih_signal = 1 THEN 1 ELSE 0 END) AS wih_exists,
                    max(CASE WHEN ned.wih_dob_adj IS NOT NULL THEN wih_dob_adj ELSE 0.0 END) AS wih_dob_adj
                FROM ds_scratch.sends_user_offer_deep_temp AS s
                LEFT JOIN ds_scratch.non_email_data_user_offer_deep_temp AS ned ON
                (
                    ned.email = s.email
                    AND 
                    (
                        (
                            ned.dt = s.cam_date - 1 
                        )
                        OR ned.dt IS NULL
                    )
                )
                WHERE substring(md5(s.email), 1, 2) >= '60' 
                AND substring(md5(s.email), 1, 2) <= '7f'
                GROUP BY s.email, s.offer_id, s.network_code, s.cam_date
            ) AS t1
        ) AS t2
        ORDER BY email ASC, offer ASC, date ASC
    ''', file_format=file_format, pprint=True)
    pprint(db.s3_queue)
    
    while True:
        key = db.fetch()

        if key:
            watch = timer()
            for i in key:
                i
            watch.print_lap('m')
        else:
            break
