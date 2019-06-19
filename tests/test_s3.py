import sys

sys.path.insert(0, '..')

from db_utils.s3_connect import s3_connect, snowflake_dmpky
from pprint import pprint
import os
import re

#config_file = os.path.join(os.environ['HOME'], '.databases.conf')
s3 = s3_connect(section='s3')

# #a = s3.get_contents('joe/1x_splits.csv', stringIO=True)

# # print(a.readline())

s3.download_file('joe/1x_splits.csv', dest_file='/home/joe/Desktop/s3_out')

a = s3.list_keys(prefix='joe/1x_splits')
print(a)
# # b = s3.list_keys(prefix='joecat/920d723a-4890-42ac-85ce-16961918e655')



# # new_list = [snowflake_dmpky(i) for i in b]


# # new_list.sort()


# # for i in new_list:
# # 	print(i)
# # new_list.reverse()
# # pprint((new_list))

# # pprint(new_list.pop())
# # pprint(new_list.pop())
# # pprint(new_list.pop())
# # pprint(new_list.pop())

# # pprint('\n\n\n\n')

# # a.sort()
# # pprint(a)


# # # pprint(s3_key('joecat/920d723a-4890-42ac-85ce-16961918e655_0_0_10.csv') > s3_key('joecat/920d723a-4890-42ac-85ce-16961918e655_0_0_2.csv'))

# # s3.download_file('joecat/920d723a-4890-42ac-85ce-16961918e655_0_0_44.csv')
# # s3.download_file('joecat/920d723a-4890-42ac-85ce-16961918e655_0_1_0.csv')
# # s3.download_file('joecat/920d723a-4890-42ac-85ce-16961918e655_0_7_45.csv')
# # s3.download_file('joecat/920d723a-4890-42ac-85ce-16961918e655_1_0_0.csv')


# # a = snowflake_dmpky('db_utils/7ad25bf2-05e0-4c6a-8297-812dfb17e032_0_0_2.csv')

# # print(a < b)

# b = snowflake_dmpky('db_utils/7ad25bf2-05e0-4c6a-8297-812dfb17e032_0_0_10.csv')

# a = s3.upload_model('test', '920d723a-4890-42ac-85ce-16961918e655_1_0_0.csv', 'input')

# pprint(a)