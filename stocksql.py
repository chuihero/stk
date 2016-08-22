#!/usr/bin/python3
# coding=utf-8

import easyhistory
import easyutils
import pymysql
import os
import stockinfo
from itertools import islice
import json
from datetime import datetime
import time
from multiprocessing.pool import ThreadPool
import threading
import sys


class Sqlconn():
    DATABASENAME = 'stock'
    BASICKTABLENAME = 'basic'
    DAYHISTORYTABLENAME = 'dayHistory'
    HISTORYPATH = 'history'

    def __init__(self,sqlConfigFile='localSqlConfig.json',char='utf8'):
        try:
            with open(sqlConfigFile) as f:
                sqlConfig = json.load(f)
        except:
            print('无法载入SQL连接配置信息，请检查')
            sys.exit()

        try:
            self.conn = pymysql.connect(host=sqlConfig['host'], \
                                        user=sqlConfig['user'], \
                                        password=sqlConfig['password'], \
                                        charset=char)
        except Exception as e:
            print(e)
            print('无法打开数据库连接')
            sys.exit()

        self.cur = self.conn.cursor()
        if self.isDatabaseExist():
            self.cur.execute('use {}'.format(self.DATABASENAME))
        else:
            self.initAll()


    def isDatabaseExist(self):
        self.cur.execute('show databases;')
        res = self.cur.fetchall()
        if (self.DATABASENAME,) not in res:
            return False
        else:
            return True


    def initAll(self):
        self.initDatabase()
        self.initBasicTable()
        self.initDayHistoryTable()

    def initDatabase(self):
        self.cur.execute('show databases;')
        res = self.cur.fetchall()
        if not (self.DATABASENAME,) in res:
            # 不存在数据库，需要新建
            self.cur.execute('create database ' + self.DATABASENAME)
        self.cur.execute('use {}'.format(self.DATABASENAME))
        return

    def initBasicTable(self):
        cur = self.cur
        cur.execute('show tables;')
        res = cur.fetchall()
        if (self.BASICKTABLENAME,) not in res:
            line = '''create table {}(
                            code char(6) primary key NOT NULL,
                            name char(20),
                            updateDate TIMESTAMP,
                            isHS300 tinyint);'''.format(self.BASICKTABLENAME)
            cur.execute(line)




    def initDayHistoryTable(self):
        cur = self.cur
        cur.execute('show tables;')
        res = cur.fetchall()
        if (self.DAYHISTORYTABLENAME,) not in res:
            line = '''create table {}(
                        code char(6) NOT NULL,
                        date Date NOT NULL,
                        open float(12,4),
                        high float(12,4),
                        close float(12,4),
                        low float(12,4),
                        volume double(16,1),
                        amount double(16,1),
                        factor float(12.4),
                        PRIMARY KEY(code,date))'''.format(self.DAYHISTORYTABLENAME)
            cur.execute(line)



    def updateBasicTable(self):
        #如果已经有表了，需要重新建
        s = 'select * from %s'%(self.BASICKTABLENAME)
        self.cur.execute(s)
        res = self.cur.fetchall()

        if len(res) != 0:
            s = 'drop table %s'%self.BASICKTABLENAME
            self.cur.execute(s)
            self.initBasicTable()

        #沪深300信息
        hs300list = stockinfo.getHS300Infor()
        #沪市信息
        shStockinfo = stockinfo.getSHStockInfo()
        for i in shStockinfo:
            s = '''insert {}(code, name,isHS300) values('{}','{}',{})'''.format(self.BASICKTABLENAME,\
                                                                            i[2],\
                                                                            i[3],\
                                                                            int((i[2] in hs300list)))
            self.cur.execute(s)

        #深市信息
        szStockinfo = stockinfo.getSZStockInfo()
        num = 0;
        for i in szStockinfo:
            if i[8] == '0':
                #B股，不予考虑
                continue
            s = '''insert {}(code, name,isHS300) values('{}','{}',{})'''.format(self.BASICKTABLENAME,\
                                                                            i[0],\
                                                                            i[1], \
                                                                            int((i[0] in hs300list)))


            self.cur.execute(s)

        self.conn.commit()

    def updateDayHistoryTable(self):
        day = easyhistory.day.Day(path=self.HISTORYPATH)
        s = 'select code from {}'.format(self.BASICKTABLENAME)
        codecur = self.conn.cursor()
        codecur.execute(s)
        codes = []
        for each in codecur:
            code = each[0]
            codes.append(code)

            rawfile = os.path.join(self.HISTORYPATH, 'day', 'raw_data', '{}.csv'.format(code))

            latestDate = self.getLatestHistoryDate(code)
            if latestDate == None:
                latestDate = datetime.date(datetime(1990, 1, 1))

            if stockinfo.needUpdate(latestDate):
                try:
                    day.update_single_code(code)
                except:
                    day.init_stock_history(code)
                    day.update_single_code(code)

            fh = open(rawfile, 'r')

            for l in islice(fh, 1, None):
                line = l.split(',')
                t = time.strptime(line[0], '%Y-%m-%d')
                y, m, d = t[0:3]
                date = datetime.date(datetime(y, m, d))
                if date <= latestDate:
                    continue

                s = '''insert {} values('{}','{}',{},{},{},{},{},{},{})'''.format( \
                    self.DAYHISTORYTABLENAME, \
                    code, \
                    line[0], \
                    line[1], \
                    line[2], \
                    line[3], \
                    line[4], \
                    line[5], \
                    line[6], \
                    line[7].rstrip())
                self.cur.execute(s)
            self.conn.commit()
            fh.close()
            print('股票{}已经更新入库'.format(code))


    def updateOneHistory(self,code):

        day = easyhistory.Day(path=self.HISTORYPATH)

        rawfile = os.path.join(self.HISTORYPATH, 'day', 'raw_data', '{}.csv'.format(code))

        latestDate = self.getLatestHistoryDate(code)
        if latestDate == None:
            latestDate = datetime.date(datetime(1990, 1, 1))

        if stockinfo.needUpdate(latestDate):
            try:
                day.update_single_code(code)
            except:
                day.init_stock_history(code)
                day.update_single_code(code)

        fh = open(rawfile, 'r')

        for l in islice(fh, 1, None):
            line = l.split(',')
            t = time.strptime(line[0], '%Y-%m-%d')
            y, m, d = t[0:3]
            date = datetime.date(datetime(y, m, d))
            if date <= latestDate:
                continue

            s = '''insert {} values('{}','{}',{},{},{},{},{},{},{})'''.format( \
                self.DAYHISTORYTABLENAME, \
                code, \
                line[0], \
                line[1], \
                line[2], \
                line[3], \
                line[4], \
                line[5], \
                line[6], \
                line[7].rstrip())
            self.cur.execute(s)
        self.conn.commit()
        fh.close()
        print('股票{}已经更新入库'.format(code))


    def getLatestHistoryDate(self,code):
        s = "select max(date) from {} where code = '{}'".format(self.DAYHISTORYTABLENAME,code)
        self.cur.execute(s)
        res =  self.cur.fetchall()


        return res[0][0]

    def reloadStock(self,code):
        s = 'delete from {} where code = {}'.format(self.DAYHISTORYTABLENAME,code)
        self.cur.execute(s)
        self.conn.commit()
        self.updateOneHistory(code)

    def updateStocksAndCommitDatabase(self):
        easyhistory.update()

        s = 'select code from {}'.format(self.BASICKTABLENAME)
        codecur = self.conn.cursor()
        codecur.execute(s)
        codes = []
        for each in codecur:
            code = each[0]
            codes.append(code)

            rawfile = os.path.join(self.HISTORYPATH, 'day', 'raw_data', '{}.csv'.format(code))

            fh = open(rawfile, 'r')

            for l in islice(fh, 1, None):
                line = l.split(',')
                t = time.strptime(line[0], '%Y-%m-%d')
                y, m, d = t[0:3]
                date = datetime.date(datetime(y, m, d))
                if date <= latestDate:
                    continue

                s = '''insert {} values('{}','{}',{},{},{},{},{},{},{})'''.format( \
                    self.DAYHISTORYTABLENAME, \
                    code, \
                    line[0], \
                    line[1], \
                    line[2], \
                    line[3], \
                    line[4], \
                    line[5], \
                    line[6], \
                    line[7].rstrip())
                self.cur.execute(s)
            self.conn.commit()
            fh.close()
            print('股票{}已经更新入库'.format(code))


# def fastUpdateThred(code):
#     sql = Sqlconn()
#     sql.updateOneHistory(code)
#
# class FastUpdate(Sqlconn):
#     def __init__(self,thrdNum=10):
#         super(Sqlconn,self).__init__()
#         sql = Sqlconn()
#         s = 'select code from {}'.format(self.BASICKTABLENAME)
#         sql.cur.execute(s)
#         self.codes = []
#         for each in sql.cur:
#             self.codes.append(each[0])
#         self.thrdNum = thrdNum
#
#     def update(self):
#         pool = ThreadPool(self.thrdNum)
#         pool.map(fastUpdateThred,self.codes)



if __name__ =='__main__':

    sql = Sqlconn()
    # 如果重新初始化数据库，需要执行下面两句。如果仅是更新数据，下面两句不用执行
    sql.initAll()
    sql.updateBasicTable()
    sql.updateDayHistoryTable()
    # FastUpdate().update()
