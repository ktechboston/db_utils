# Imports
from datetime import datetime
from pprint import pprint
import io
import configparser
import boto3
import csv
import os

# S3 Object
class s3_connect(object):

    def __init__(self, config_file, section):
        '''
        provide config file with options:
            aws_access_key_id
            aws_secret_access_key
            default_bucket
        section = config section for s3

        example:
        [s3]
            aws_access_key_id=<key_id>
            aws_secret_access_key=<secret_key>
            default_bucket=<bucket>

        Basic Usage:

        >>> from db_utils.s3_connect import s3_connect
        >>>
        >>> s3 = s3_connect('.databases.conf', 's3')
        >>> s3.list_keys(prefix='examples')

        '''
        creds = configparser.ConfigParser()
        self.config_file = config_file
        self.section = section
        creds.read(self.config_file)
        self.DEFAULT_BUCKET=creds.get(self.section, 'default_bucket')
        self.access_key = creds.get(self.section, 'aws_access_key_id')
        self.secret_key = creds.get(self.section, 'aws_secret_access_key')
        
        self.conn = boto3.resource(
            's3',
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
        )


    def list_keys(self, bucket=None, prefix=None):
        '''
        bucket = defaults_bucket in config file
        prefix = filters by prefix if provided
        returns list of keys in specified bucket
        '''
        if bucket == None:
            bucket = self.DEFAULT_BUCKET

        bk = self.conn.Bucket(bucket)
        key_list = []


        if prefix:
            return [key.key for key in bk.objects.filter(Prefix=prefix)]
        else:
            return [key.key for key in bk.objects.all()]


    def upload_file(self, file_location, key, bucket=None):
        '''
        file_location = path to file
        key = desired s3 key
        bucket = defaults_bucket in config file
        '''
        if bucket == None:
            bucket = self.DEFAULT_BUCKET

        self.conn.Bucket(bucket).upload_file(file_location, key)
        return '{0}/{1}'.format(bucket, key)


    def upload_model(self, model, file_location, model_type, bucket=None, pkl_model_type='', append_ts=False, suffix='csv'):
        '''
        bucket = optional, defaults to 'default_bucket' section in config file
        
        model = comma delimited list of valid model names in .databases.conf file

        file_location = path to file

        model_type = (input | output | pickled_model)
        pkl_model_type = (scaler |random_forest | collaborative_filter)

        append_ts = Boolean appends SQL friendly timestamp col to CSV file
                    YYYY-MM-DD HH:MM:SS

        uploads to s3://<bucket>/models/<model>/(input | output)/<year>/<month>/<day>_<type><timestamp>.csv
        returns dict {'bucket': <bucket>, 'key': <key>}

        '''
        creds = configparser.ConfigParser()
        self.config_file = config_file
        creds.read(self.config_file)
        valid_models = tuple(creds.get(self.section, 'model'))

        types = ('input', 'output', 'pickled_model')

        pkl_model_types = ('scaler', 'random_forest', 'collaborative_filter', 'lstm', 'deep')

        if model not in valid_models:
            raise TypeError('model must be {0}'.format(valid_models))

        if model_type not in types:
            raise TypeError('model_type must be {0}'.format(types))

        if model_type == 'pickled_model' and pkl_model_type == '':
            raise TypeError('pkl_model_type must be of type {0}'.format(pkl_model_types))
        elif model_type == 'pickled_model' and pkl_model_type not in pkl_model_types:
            raise TyperError('pkl_model_type must be {0}'.format(pkl_model_types))


        if bucket == None:
            bucket = self.DEFAULT_BUCKET

        time = datetime.now()
        params = {
            "year": time.strftime('%Y'),
            "month": time.strftime('%m'),
            "day": time.strftime('%d'),
            "time": time.strftime('%H:%M:%S'),
            "model": model,
            "model_type": model_type,
            "pkl_model_type": pkl_model_type,
            "suffix": suffix
        }

        if append_ts == True:
            timestamp = '{year}-{month}-{day} {time}'.format(**params)
            f_out_name = file_location.split('.')[0] + '_appended.txt'
            f_out = open(f_out_name, 'w')
            with open(file_location, 'r+') as csvfile:
                for row in csvfile:
                    out_row = row.strip() + ',' + timestamp + '\n'
                    f_out.write(out_row)
            f_out.close()
            os.remove(file_location)
            os.rename(f_out_name, file_location)
            print('timestamps appended to ' + file_location)


        key = 'models/{model}/{model_type}/{year}/{month}/{pkl_model_type}{day}_{time}.{suffix}'.format(**params)
        self.upload_file(file_location, key, bucket=bucket)

        return {'bucket': bucket, 'key': key, 'params': params}


    def copy(self, copy_key, dest_key, bucket=None):
        '''
        copy_key = source key to copy
        dest_key = destination key
        bucket = optional, defaults to 'default_bucket' section in config file
        '''
        if bucket == None:
            bucket = self.DEFAULT_BUCKET

        copy_source = {
            'Bucket': bucket,
            'Key': copy_key
        }

        print(self.conn.meta.client.copy(copy_source, bucket, dest_key))
        return 's3://{0}/{1}'.format(bucket, dest_key)


    def download_file(self, key, dest_file=None, bucket=None):
        '''
        key = s3 key
        dest_file = optional path to desired file output
        bucket = optional, defaults to 'default_bucket' section in config file
        '''

        filename = key.split('/')[-1]

        if bucket == None:
            bucket = self.DEFAULT_BUCKET

        if dest_file == None:
            path = key.split('/')[-1]
        else:
            path = dest_file


        bucket = self.conn.Bucket(bucket)
        bucket.download_file(key, path)

        return path


    def get_contents(self, key, bucket=None, stringIO=False):
        '''
        key <string> - s3 key

        bucket <string> - optional, defaults to 'default_bucket' section in config file

        stringIO <boolean> - optional, will return stringIO stream

        returns BytesIO stream
        '''


        io_bytes = io.BytesIO()
        if bucket == None:
            bucket = self.DEFAULT_BUCKET

        bucket = self.conn.Bucket(bucket)
        bucket.download_fileobj(key, io_bytes)

        if stringIO == True:
            io_string = io.StringIO(io_bytes.getvalue().decode('utf-8'))
            return io_string

        return io_bytes


    def del_key(self, key, bucket=None):
        '''
        key = s3 key
        bucket = optional, defaults to 'default_bucket' section in config file
        '''
        if bucket == None:
            bucket = self.DEFAULT_BUCKET

        return self.conn.Object(bucket, key).delete()
