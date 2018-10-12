# db_utils library

## Installation

```
pip install db_utils
```

### DBUtil class
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

    >>> from db_utils.DBUtil import DBUtil
    >>>
    >>> db = DBUtil('redshift_example', '.databases.conf')
    >>> db.get_arr_from_query('select * from test', pprint=True)
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