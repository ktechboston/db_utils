from db_utils.s3_connect import s3_connect
import os


config_file = os.path.join(os.environ['HOME'], '.databases.conf')
s3 = s3_connect(config_file, 's3')

a = s3.get_contents('joe/1x_splits.csv', stringIO=True)

print(a.readline())

s3.download_file('joe/1x_splits.csv', dest_file='/home/joe/Desktop/s3_out')