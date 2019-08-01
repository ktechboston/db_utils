sleep 15\
&& cd /root/tests\
&& python3 test_mysql.py\
&& python3 test_pg_connect.py\
&& python3 test_sqlite.py