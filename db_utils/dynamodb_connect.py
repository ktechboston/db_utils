import boto3
import os
import configparser
import json
from pprint import pprint
from decimal import Decimal
from boto3.dynamodb.types import TypeSerializer, TypeDeserializer
from db_utils.DBUtil import DBUtil



class dynamodb_connect():

    def __init__(self, config_file, section):
        '''
        provide config file with options:
            aws_access_key_id
            aws_secret_access_key
            region=
        

        example:
        [dynamodb]
            aws_access_key_id=<key_id>
            aws_secret_access_key=<secret_key>
            region=<aws region>

        '''

        self.config_file = config_file
        self.serializer = TypeSerializer()
        self.deserializer = TypeDeserializer()
        config_file = os.path.join(os.environ['HOME'], '.databases.conf')
        creds = configparser.ConfigParser()
        creds.read(config_file) 
        
        self.conn = boto3.resource(
            'dynamodb',
            aws_access_key_id=creds.get(section, 'aws_access_key_id'),
            aws_secret_access_key=creds.get(section, 'aws_secret_access_key'),
            region_name=creds.get(section, 'region')
        )

        self.client = boto3.client(
            'dynamodb'
        )


    def get_table_creation_ts(self, table_name):
        '''
        table_name <string> 

        returns datetime object of when table was created
        '''

        table = self.conn.Table(table_name)
        return table.creation_date_time


    def put_item(self, **kwargs):
        '''
            kwargs:
                table_name <string>[required]
                dict <dict> [required]
                    Primary key within dict required

        inserts dictionary into given table in dynmodb
        returns response object
        '''

        if ('table_name' or 'dict') not in kwargs:
             raise TypeError('missing required kwargs')
        
        table = self.conn.Table(kwargs['table_name'])
        request = self.recursive_dec_convert(kwargs['dict'])
        response = table.put_item(Item=request)
        
        return response


    def query(self, table_name, **kwargs):
        '''
        table_name <string>
        
        kwargs:
            key <required> the primary key name of table
            attribute 
            operator <optional> default value  'EQ'
                                'NE'|'IN'|'LE'|'LT'|'GE'|
                                'GT'|'BETWEEN'|'NOT_NULL'|'NULL'
                                |'CONTAINS'|'NOT_CONTAINS'|'BEGINS_WITH'
        '''

        if not kwargs.get('operator'):
            kwargs['operator'] = 'EQ'


        value = self.serializer.serialize(kwargs['attribute'])

        response = self.client.query(
            TableName=table_name,
            KeyConditions={kwargs['key']:{
                                    "ComparisonOperator": kwargs['operator'],
                                    "AttributeValueList": [value]
                                    }
                            }
                            )

        return response['Items']


    def batch_write_item(self, table_name, items):
        '''
            table_name = <string> name of table name in dynamodb
            items = <list> lists of dictionaries
        '''
        
        #construct dynamo batch write request for single table        
        r = [{"PutRequest": {"Item": self.convert_to_dynamo_struct(item)['M']}} for item in items ]

        request = {
                    table_name: r
                }

        return self.client.batch_write_item(RequestItems=request)


    def recursive_dec_convert(self, obj):
        '''
        helper function that recursively type casts float data types to 
        Decimals
        '''
        if isinstance(obj, list):
            for i in range(len(obj)):
                obj[i] = self.recursive_dec_convert(obj[i])
            return obj
        elif isinstance(obj, dict):
            for k in obj.keys():
                obj[k] = self.recursive_dec_convert(obj[k])
            return obj
        elif isinstance(obj, float) or isinstance(obj, int):
            return Decimal(str(obj))
        else:
            return obj


    def convert_to_dynamo_struct(self, obj):
        '''
        helper function that recursively converts python obj to
        dynmodb acceptable format

        ex)

        {'email': 'rowBoi88@yahoo.com',
         'network_code': 'GW',
         'network_offer_id': 'GW9472',
         'offer_id': '9472',
         'rank': 1,
         'recommender_score': 0.968518495559692}

         {'M': {'email': {'S': 'rowBoi88@yahoo.com'},
       'network_code': {'S': 'GW'},
       'network_offer_id': {'S': 'GW9472'},
       'offer_id': {'S': '9472'},
       'rank': {'N': '1'},
       'recommender_score': {'N': '0.968518495559692'}}}
        '''
        no_float = self.recursive_dec_convert(obj)
        return self.serializer.serialize(no_float)