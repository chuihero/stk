#!/usr/bin/python3

# coding=utf-8

import pymysql
import pandas as pd
from datetime import datetime
import json
import sys
import talib
import matplotlib.pyplot as plt

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
    def getDayLines(self,code,yearOffset = 3,monthoffset = 0,dayoffset=0):
        """

        :param code: 股票代码
        :param yearOffset: 年份偏执，只需近几年的股票数据
        :return:
        """
        assert type(code)==str, '股票代码应为字符串'
        self.code = code
        today = datetime.today()
        startDate = datetime(today.year - yearOffset,\
                             today.month - monthoffset,\
                             today.day - dayoffset)

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

    def getStockList(self):
        s = 'select code from {}'.format(self.BASICKTABLENAME)
        self.cur.execute(s)
        f = self.cur.fetchall()
        res = []
        for i in f:
            res.append(i[0])
        return res


    def getHS300StockList(self):
        s = 'select code from {} where isHS300=1'.format(self.BASICKTABLENAME)
        self.cur.execute(s)
        f = self.cur.fetchall()
        res = []
        for i in f:
            res.append(i[0])
        return res

class VisualTool():
    def __init__(self):
        self.basicFig = plt.figure(facecolor='white', figsize=(14, 10))

    def basicPlot(self):


        textsize = 9
        fillcolor = 'darkgoldenrod'
        left, width = 0.05, 0.9
        rect1 = [left, 0.5, width, 0.45]
        rect2 = [left, 0.275, width, 0.225]
        rect3 = [left, 0.05, width, 0.225]

        axescolor = '#f6f6f6'

        ax1kline = self.basicFig.add_axes(rect1, axisbg=axescolor)
        ax1volume = ax1kline.twinx()
        ax2macd = self.basicFig.add_axes(rect2, axisbg=axescolor, sharex=ax1kline)
        ax3boll = self.basicFig.add_axes(rect3, axisbg=axescolor, sharex=ax1kline)

        ##plot k lines


        v = self.df

        up = v['close'] >= v['open']
        dw = ~up
        ax1kline.vlines((v.ix[up]).index, v.ix[up, 'high'], v.ix[up, 'low'], color='red', linewidth=1,
                        label='_nolegend_')
        ax1kline.vlines((v.ix[up]).index, v.ix[up, 'open'], v.ix[up, 'close'], color='red', linewidth=3,
                        label='_nolegend_')
        ax1kline.vlines((v.ix[dw]).index, v.ix[dw, 'high'], v.ix[dw, 'low'], color='green', linewidth=1,
                        label='_nolegend_')
        ax1kline.vlines((v.ix[dw]).index, v.ix[dw, 'open'], v.ix[dw, 'close'], color='green', linewidth=3,
                        label='_nolegend_')

        ma = v.ma1
        ax1kline.plot(ma.index, ma, color='y', label=('MA(%s)' % self.maList[0]))
        ma = v.ma2
        ax1kline.plot(ma.index, ma, color='g', label=('MA(%s)' % self.maList[1]))
        ax1kline.grid()
        ax1kline.set_title('%s' % self.code)

        volume = v['volume']
        vmax = volume.max()
        poly = ax1volume.bar(volume.index, volume, width=0.4, label='Volume', facecolor=fillcolor, edgecolor=fillcolor)
        ax1volume.set_ylim(0, 5 * vmax)
        ax1volume.set_yticks([])

        ## plot macd


        ax2macd.plot(v.index, v['dif'], color='m', lw='1')
        ax2macd.plot(v.index, v['dea'], color='c', lw='1')
        redmacd = v['macd'] >= 0
        ax2macd.bar(v.ix[redmacd].index, v.ix[redmacd, 'macd'], width=0.2, color='r', ec='r')
        greenmacd = v['macd'] < 0
        ax2macd.bar(v.ix[greenmacd].index, v.ix[greenmacd, 'macd'], width=0.2, color='g', ec='g')
        ax2macd.grid()


    def drawLines(self,ax,value,colors):
        if value == None:
            return
        assert len(value)==len(colors), '颜色和数据不一样长'
        for i in range(len(value)):
            ax.axhline(value[i],color=colors[i])




    def drawRectangle(self):
        pass;