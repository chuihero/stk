#!/usr/bin/python3
# coding=utf-8

import easyhistory
import easyutils
import pymysql
import os
import stockinfo
from itertools import islice
import json



class Sqlconn():
    DATABASENAME = 'stock'
    BASICKTABLENAME = 'stockbasic'
    DAYHISTORYTABLENAME = 'stockDayHistory'
    HISTORYPATH = 'history'

    def __init__(self,ip= '127.0.0.1',usr='root', psw='worship',char='utf8'):
        try:
            self.conn = pymysql.connect(host=ip,user=usr,password=psw,charset=char)
        except:
            print('无法连接数据库！')
            return
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
                        code char(6) primary key NOT NULL,
                        date Date primary key NOT NULL,
                        open float(8,4),
                        high float(8,4),
                        close float(8,4),
                        low float(8,4),
                        volume float,
                        amount float,
                        factor float)'''.format(self.DAYHISTORYTABLENAME)
            cur.execute(line)



    def updateBasicTable(self):
        #如果已经有表了，需要重新建
        s = 'select * from %s'%(self.BASICKTABLENAME)
        self.cur.execute(s)
        res = self.cur.fetchall()

        if len(res) != 0:
            s = 'drop table %s'%self.BASICKTABLENAME
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
            s = '''insert {}(code, name,isHS300) values('{}','{}',{})'''.format(self.BASICKTABLENAME,\
                                                                            i[0],\
                                                                            i[1], \
                                                                            int((i[0] in hs300list)))


            self.cur.execute(s)

        self.conn.commit()

    def updateDayHistoryTable(self):
        day = easyhistory.day.Day(path=self.HISTORYPATH)
        histroypath = os.path.join(self.HISTORYPATH,'day','data')
        rawpath = os.path.join(self.HISTORYPATH,'day','raw_data')

        for file in os.listdir(histroypath):
            code  = file[0:6]
            fh = open(os.path.join(histroypath,file),'r')
            latestDate = day.store.get_his_stock_date(code)

            for line in islice(fh, 1, None):
                s =''










if __name__ =='__main__':

    sql = Sqlconn()
    # sql.initDatabase()
    # sql.updateBasicTable()
    sql.updateDayHistoryTable()
