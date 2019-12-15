import csv
import pandas as pd
import numpy as np
import sys
import taos
import os

data = pd.read_csv('RAILAFC_20170416_BAK.csv',encoding = 'GB2312')

def exitProgram(conn):
    conn.close()
    sys.exit()

if __name__ == '__main__':

    conn = taos.connect(host="127.0.0.1", user="root", password="taosdata", config="/etc/taos")
    
    c1 = conn.cursor()

    try:
        c1.execute('create database if not exists dbcard')
    except Exception as err:
        conn.close()
        raise(err)
        
    try:
        c1.execute('use dbcard')
    except Exception as err:
        conn.close()
        raise(err)
    for i in range(len(data)):
        try:
            c1.execute('create table if not exists c' + str(data['卡发行号'][i]) + '(imtime timestamp, time int, city int, job int, imline int, imstat int, date int, exline int, exstat int)')
        except Exception as err:
            conn.close()
            raise(err)
 
    for i in range(len(data)):
        try:
            str1=''
            str1=str(data['进站时间'][i])
            c1.execute("insert into c" + str(data['卡发行号'][i]) + " values ('%s', %d, %d, '%d', '%d', '%d', '%d', '%d', '%d')" % (str1[0:4]+'-'+str1[4:6]+'-'+str1[6:8]+' '+str1[8:10]+':'+str1[10:12]+':'+str1[12:14], data['交易时间'][i], data['城市编码'][i], data['行业编码'][i], data['进站线路号'][i], data['进站站码'][i], data['交易日期'][i], data['出站线路号'][i], data['出站站码'][i]))
        except Exception as err:
            conn.close()
            raise(err)
 
'''try:
        c1.execute('use dbcard')
        c1.execute('show tables')
    except Exception as err:
        conn.close()
        raise(err)
 
    cols1 = c1.description
    data1 = c1.fetchall()
 
    try:
        c1.execute('select * from c00846218')
    except Exception as err:
        conn.close()
        raise(err)

    for col in c1:
        print(col)'''
 
    conn.close()


pd.set_option('mode.chained_assignment', None)
