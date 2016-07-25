#!/usr/bin/python3
# coding=utf-8

import pymysql
import pandas as pd
from datetime import datetime
import json
import sys

class Client():
    DATABASE = 'stock'
    BASICKTABLENAME = 'basic'
    DAYHISTORYTABLENAME = 'dayHistory'

    def __init__(self, sqlConfigFile='sqlConfig.json', char='utf8'):
        try:
            with open(sqlConfigFile) as f:
                sqlConfig = json.load(f)
        except:
            print('无法载入SQL连接配置信息，请检查')

        try:
            self.conn = pymysql.connect(host=sqlConfig['host'], \
                                        user=sqlConfig['user'], \
                                        password=sqlConfig['password'], \
                                        database=self.DATABASE, \
                                        charset=char)
        except Exception as e:
            print(e)
            print('无法打开数据库连接')
            sys.exit()

        self.cur = self.conn.cursor()
    def getDayLines(self,code,yearOffset = 3):
        """

        :param code: 股票代码
        :param yearOffset: 年份偏执，只需近几年的股票数据
        :return:
        """
        assert type(code)==str, '股票代码应为字符串'
        self.code = code
        today = datetime.today()
        startDate = datetime(today.year - yearOffset,\
                             today.month,\
                             today.day)

        s = "select @f:=max(factor) from {} where code = '{}'".format(\
                                            self.DAYHISTORYTABLENAME,\
                                            self.code)
        self.cur.execute(s)
        s = "select date, open/@f as open, high/@f as high,close/@f as close," \
            "low/@f as low, volume, amount/@f as amount from {} " \
            "where code='{}' and date >='{}'".format(self.DAYHISTORYTABLENAME,\
                                                     self.code,
                                                     startDate.strftime('%y-%m-%d'))
        df = pd.read_sql(s,self.conn)
        return df


if __name__=='__main__':
    c = Client()
    a = c.getDayLines('000001')
    print(a)

