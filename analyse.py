#!/usr/bin/python3
# coding=utf-8

import pymysql
import pandas as pd
from datetime import datetime
import json
import sys
import talib

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
            "where code='{}' and date >='{}' " \
	        "order by date".format(self.DAYHISTORYTABLENAME,\
                                   self.code,
                                   startDate.strftime('%y-%m-%d'))
        df = pd.read_sql(s,self.conn)
        return df

class Chan():
    def __init__(self,code):
        assert type(code) == str, '股票代码应为字符串'
        self.code = code

        client = Client()
        self.df = client.getDayLines(self.code)

    def filterKLines(self):
        """根据缠论，把k线过滤一遍，删掉包含关系的k线"""


        def isInclude(last,next):
            return last['high']>=next['high'] and last['low']<=next['low']

        def isIncludedBy(last,next):
            return last['high']<=next['high'] and last['low']>=next['low']

        def popLastReserved():
            pass;

        def pushLatest():
            pass;
        def findAllReserved(x,l):
            return [a for a in range(len(l)) if l[a]==x]

        filteredRes = [True,]
        last = self.df.iloc[0]

        for i in range(1,len(self.df)):
            next = self.df.iloc[i]
            if isInclude(last,next):
                filteredRes.append(False)
                continue

            elif isIncludedBy(last,next):
                # 需要回退，直到不再被包含
                truePosition = findAllReserved(True,filteredRes)
                while len(truePosition)>=1:
                    p = truePosition.pop()
                    filteredRes[p] = False
                    last = self.df.iloc[truePosition[-1]]
                    if isIncludedBy(last,next):
                        if len(truePosition) == 0:
                            #回退到第一个
                            last = next
                            filteredRes.append(True)
                            break

                        continue
                    elif isInclude(last,next):
                        filteredRes.append(False)
                    else:
                        filteredRes.append(True)
                        last = next
                    break
                continue
            else:
                filteredRes.append(True)
                last = next
                continue

        self.df.loc[:,'reserved'] = filteredRes
        return self

    def findPeakBottom(self,gap=4):
        """
        找到k线中的高点和低点
        :param :
            gap:顶分和底分之间的间隔，默认为4
        :return:
        """

        def isRise(next,last):
            return next['high']>=last['high']

        def isPeak(next,last,status):
            return (not isRise(next,last)) and status =='rise'

        def isBottom(next,last,status):
            return (isRise(next,last)) and status == 'fall'

        res = []
        filteredDf = self.df[self.df['reserved']==True]

        last = filteredDf.iloc[0]
        status = ''
        for i in range(1,len(filteredDf)):
            next = filteredDf.iloc[i]
            if isPeak(next,last,status):
                #stock rise
                res.append('peak')
            elif isBottom(next,last,status):
                res.append('bottom')
            else:
                #普通上涨或下跌，但不是顶分或底分
                res.append('rise' if isRise(next,last) else 'fall')

            status = 'rise' if isRise(next,last) else 'fall'
            last = next
            continue
        filteredDf.loc[1:,'fenxing'] = res
        print(filteredDf)

if __name__=='__main__':
    c = Chan('000001')
    c.filterKLines()
    c.findPeakBottom()

