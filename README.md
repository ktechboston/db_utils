# db_utils library

## Installation

```
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

```


### db_connect class
Parent python database connectin class utilizing
API specification v2.0 https://www.python.org/dev/peps/pep-0249/#connection-methods
use the connection classes above specific to the flavor of db you're using


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