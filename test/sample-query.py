#!/usr/bin/env python

#import numpy
import psycopg2
#import sys

dbname = 'desc_dc2_drp'
username = 'desc_dc2_drp_user'
dbhost = 'nerscdb03.nersc.gov'

dbconfig = {'dbname' : dbname, 'user' : username, 'host' : dbhost}
dbconn = psycopg2.connect(**dbconfig)

schema = 'run11p'

with dbconn.cursor() as cursor:
    try:
        cursor.execute('SELECT tract, COUNT(object_id) from {schema}.forced where isprimary group by tract'.format(**locals()))
        print('tract    count')
        for record in cursor:
            print(record[0], ': ', record[1])
        for table in ['forced', 'forced2', 'forced3']:
            exec_string = "SELECT table_name, column_name, data_type from information_schema.columns where table_schema='{schema}' and table_name='{table}' limit 15".format(**locals())
            print(exec_string)
            cursor.execute(exec_string)
            print('type   table    column')
            for record in cursor:
                print(record[2], '  ', record[0], '  ', record[1])
    except:
        raise



