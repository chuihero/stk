#!/usr/bin/python3
# coding=utf-8

import easyhistory
import pymysql
import os
from bs4 import BeautifulSoup

class Sqlconn():
    def __init__(self,ip= '127.0.0.1',usr='root', psw='worship',char='utf8'):
        try:
            self.conn = pymysql.connect(host=ip,user=usr,password=psw,charset=char)
        except:
            print('无法连接数据库！')
            return

    def initDatabase(self):
        pass

    def initBasicTable(self):
        pass

    def initDayHistoryTable(self):
        pass

    def updateBasicTable(self):
        pass

    def updateDayHistoryTable(self):
        pass

