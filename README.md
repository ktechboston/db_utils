# db_utils library

## Docker
One line command to start a Docker container with db-utils installed.  This will run on any system that is running Docker.  A juypter notebook will open up on port 8888, just copy and paste the url from the terminal into your prefered browser.

```bash
docker run -p 8888:8888 hannaj06/db-utils::latest
```


## Installation

Required system packages:
 * apt-get install libpq-dev (Ubuntu)
 * apt-get install unixodbc-dev (Ubuntu)
 * brew install postgresql (MacOS)

```bash
pip install db_utils
```

### pg_connect class (previously DBUtil)
A database connection class to interact with  Postgres or Redshift

Basic Usage:
 * create database configuration file
 * example below is called .databases.conf

```
    [redshift_example]
    host=redshift.example.com
    user=test_user
    password=password
    port=5439
    database=test_db

    >>> from db_utils.pg_connect import pg_connect
    >>>
    >>> db = pg_connect('redshift_example', '.databases.conf')
    >>> db.get_arr_from_query('select * from test', pprint=True)
```


### snowflake_connect class
A database connection class to interact with snowflake

Basic Usage:
 * create database configuration file
 * example below is called .databases.conf

```
    [snowflake]
    account=abc123.us-east-1
    host=abc123.us-east-1.snowflakecomputing.com
    user=test_user
    password=password
    port=443
    database=test_db
    aws_access_key_id=<key_id>
    aws_secret_access_key=<secret_key>
```

### snowflake_s3 class
A child class of snowflake_connect class used to retrieve large datasets in small chunks

Basic Usage:
 * create database configuration file
 * example below is called .databases.conf
 * note the additional fields required

```
    [snowflake]
    account=abc123.us-east-1
    host=abc123.us-east-1.snowflakecomputing.com
    user=test_user
    password=password
    port=443
    database=test_db
    aws_access_key_id=<key_id>
    aws_secret_access_key=<secret_key>
    default_bucket=
```

example) Loading large data set into memory in chunks
```
    >>> from db_utils.snowflake_connect import snowflake_s3
    >>> import os
    >>>
    >>> file_format = '''
    TYPE = CSV
    COMPRESSION = NONE
    '''
    >>>
    >>>
    >>> with snowflake_s3('snowflake', '.databases.conf') as db:
    >>>     db.cursor('SELECT * FROM example_large_table', file_format=file_format, pprint=True)
    >>>
    >>>     while True:
    >>>         file = db.fetch(contents=True)
    >>>
    >>>         if file:
    >>>             for row in file:
    >>>                 print(row)
    >>>
    >>>         else:
    >>>             break


```


### db_connect class
Parent python database connectin class utilizing
API specification v2.0 https://www.python.org/dev/peps/pep-0249/#connection-methods
use the connection classes above specific to the flavor of db you're using


### sqlite_connect class
A database connection class to interact with SQLite

```

    >>> from db_utils.sqlite_connect import sqlite_connect
    >>>
    >>> db = sqlite_connect('test.db')
    >>> db.get_df_from_query('select * from test_table', pprint=True)
```

### s3_connect class
Connection library for interacting with S3

Basic Usage:
 * add s3 section to .databases.conf file (created in previous example)

```
    [s3]
        aws_access_key_id=<key_id>
        aws_secret_access_key=<secret_key>
        default_bucket=<bucket>


    >>> from db_utils.s3_connect import s3_connect
    >>>
    >>> s3 = s3_connect('.databases.conf', 's3')
    >>> s3.list_keys(prefix='examples')

```

example) grab file from s3 into memory as stringIO object
```
    >>> from db_utils.s3_connect import s3_connect
    >>>
    >>> s3 = s3_connect('.databases.conf', 's3')
    >>> s3.get_contents('example_file', stringIO=True)
    >>> s3.read()

```

### sql server connect class

Requirements: 
 * sql server drivers - https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-2017

Basic Usage:
 * add sql server section to .databases.conf file (created in previous example)


```
    [sql_server]
    driver=ODBC Driver 17 for SQL Server
    server=127.0.0.1
    user=bill
    password=gates
    database=master

    >>> from db_utils.sql_server_connect import sql_server_connect
    >>> db = sql_server_connect('sql_server', 'databases.conf')
    >>>
    >>> db.get_arr_from_query('''SELECT * FROM SYSOBJECTS''', pprint=True))
```



### dynamodb_connect class
Connection library for interacting with Dynamodb


### timer class
Helper class to time long running processes

Basic Usage:

```
>>> from db_utils.timer import timer
>>>
>>> t = timer()
>>> t.lap('s')
5.469961
```
