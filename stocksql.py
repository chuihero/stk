#!/usr/bin/python3
# coding=utf-8

import easyhistory
import easyutils
import pymysql
import os
from bs4 import BeautifulSoup

class Sqlconn():
    DATABASENAME = 'stock'
    BASICKTABLENAME = 'stockbasic'
    DAYHISTORYTABLENAME = 'stockDayHistory' \
                          ''
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
                            code char(6) NOT NULL,
                            name char(20),
                            updateDate TIMESTAMP,
                            isHS300 tinyint);'''.format(self.BASICKTABLENAME)
            cur.execute(line)

        self.updateBasicTable()


    def initDayHistoryTable(self):
        cur = self.cur
        cur.execute('show tables;')
        res = cur.fetchall()
        if (self.DAYHISTORYTABLENAME,) not in res:
            line = '''create table {}(
                        code char(6) NOT NULL,
                        date Date NOT NULL,
                        open float(8,4),
                        high float(8,4),
                        close float(8,4),
                        low float(8,4),
                        volume float,
                        amount float,
                        factor float)'''.format(self.DAYHISTORYTABLENAME)
            cur.execute(line)

        self.updateDayHistoryTable()

    def updateBasicTable(self):
        codeList = easyutils.stock.get_all_stock_codes()
        for code in codeList:
            line = '''insert into {}(code) values({})'''.format(self.BASICKTABLENAME,code)
            self.cur.execute(line)
        self.conn.commit()
        print('stock basic information updated!')

    def updateDayHistoryTable(self):de) v
updat
        easyhistory.update()




if __name__ =='__main__':

    sql = Sqlconn()
    # sql.initDatabase()
    sql.updateBasicTable()
    # sql.updateDayHistoryTable()
