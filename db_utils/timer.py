from datetime import datetime
import time

class timer(object):
    '''
    Helper class to time long running processes

    Basic Usage:
    
    >>> from db_utils.timer import timer
    >>>
    >>> t = timer()
    >>> t.lap('s')
    5.469961
    '''
    def __init__(self):
        self.start = datetime.now()
        self.end = None


    def lap(self, units):
        self.end = datetime.now()
        delta = self.end - self.start
        self.start = self.end

        if units == 's':
            return delta.total_seconds()

        elif units == 'm':
            return delta.total_seconds()/60

        elif units == 'h':
            return delta.total_seconds()/3600


    def print_lap(self, units):
        time = round(self.lap(units),2)

        if units == 's':
            print('{0} seconds \n\n'.format(time))

        elif units == 'm':
            print('{0} minutes \n\n'.format(time))

        elif units == 'h':
            print('{0} hours \n\n'.format(time))

            
def ts_dict(TIMESTAMP):
    '''
    returns a timestamp dict with keys:
        * year YYYY
        * month MM
        * day DD
        * hour HH
        * min MM
        * sec SS
    '''
    
    TIMESTAMP_DICT = {
            'year': TIMESTAMP.strftime('%Y'),
            'month': TIMESTAMP.strftime('%m'),
            'day': TIMESTAMP.strftime('%d'),
            'hour': TIMESTAMP.strftime('%H'),
            'min': TIMESTAMP.strftime('%M'),
            'sec': TIMESTAMP.strftime('%S')   
    }
    
    return TIMESTAMP_DICT


def sql_ts(TIMESTAMP):
    '''
    returns sql like timestamp
    
    YYYY-MM-DD HH:MM:SS
    '''
    return TIMESTAMP.strftime('%Y-%m-%d %H:%M:%S')


def s3_ts(TIMESTAMP):
    '''
    returns s3 ts format /<year>/<month>/DD_HH:MM:SS
    '''
    return TIMESTAMP.strftime('/%Y/%m/%d_%H:%M:%S')

def s3_glue_ts(TIMESTAMP):
    '''
    returns s3 AWS glue friendly key parition
    '''
    return TIMESTAMP.strftime('''/year=%Y/month=%m/day=%d/%Y-%m-%d_%H:%M:%S''')