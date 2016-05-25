#!/usr/bin/python3
# coding=utf-8
"""
该部分文件用来操作MySQL
"""

import pymysql
import httplib2
import os
from bs4 import BeautifulSoup

STOCKDATABASE = 'stockklines'
KLINETABLE = 'klinedata'
HSASTOCKTABLE = 'HSAstocks'
HA300STOCKTABLE = 'HS300stocks'


def initialDatabase(conn):

    cur = conn.cursor()
    cur.execute('show databases;')
    res = cur.fetchall()
    if not (STOCKDATABASE,) in res:
        # 不存在数据库，需要新建
        cur.execute('create database ' + STOCKDATABASE)

    return

def initialKlineTable(conn):
    cur = conn.cursor()
    cur.execute('use ' + STOCKDATABASE)

    cur.execute('show tables;')
    res = cur.fetchall()
    if (KLINETABLE,) not in res:
        sql = '''create table if not exists %s(
            date Datetime not null,
            code char(6) not null,
            name char(20),
            close float,
            high float,
            low float,
            open float,
            preClose float,
            涨跌幅 float,
            换手率 float,
            vol float,
            成交额 float,
            总市值 float,
            流通市值 float);
            '''%(KLINETABLE)
        cur.execute(sql)

def initialStockTable(conn):
    cur = conn.cursor()
    cur.execute('use '+STOCKDATABASE)
    cur.execute('show tables')
    res = cur.fetchall()
    if (HSASTOCKTABLE,) not in res:
        sql = '''create table if not exists %s(
                code char(6) not null,
                name char(20))'''%HSASTOCKTABLE

        cur.execute(sql)

    szstockurl = 'http://www.szse.cn/szseWeb/ShowReport.szse?SHOWTYPE=EXCEL&CATALOGID=1110&tab1PAGENUM=1&ENCODE=1&TABKEY=tab1'
    h = httplib2.Http('temp')
    header = {'user-agent': \
                  'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.152 Safari/537.36'}

    # resp,cont = h.request(szstockurl,headers=header)
    # if resp.status == 200:
    #     try:
    #         f = open(os.path.join('temp','szstock.html'),'wb')
    #     except:
    #         print('can not write SZ stocks to file! ')
    #     f.write(cont)
    #     f.close()

    # 读取excel文件，并导入数据库
    soup =BeautifulSoup(open(os.path.join('temp','szstock.html'),'rb'), 'html.parser')
    res = soup.find_all('td',class_='cls-data-td')
    codeList = []
    nameList =[]
    for i in range(len(res)):
        if i%20 ==0:
            codeList.append(res[i].get_text())
        if i%20 ==1:
            nameList.append(res[i].get_text())
    # print(codeList)
    # print(nameList)
    for i in range(len(codeList)):
        sql = "insert into %s values('%s','%s');"%(HSASTOCKTABLE,codeList[i],nameList[i])
        cur.execute(sql)

    conn.commit()


    shstockurl = 'http://query.sse.com.cn/security/stock/downloadStockListFile.do?csrcCode=&stockCode=&areaName=&stockType=1'
    resp, cont = h.request(szstockurl, headers=header)
    if resp.status == 200:
        try:
            f = open(os.path.join('temp', 'shstock.xls'), 'wb')
        except:
            print('can not write SH stocks to file! ')
        f.write(cont)
        f.close()



    if (HA300STOCKTABLE,) not in res:
        sql = '''create table if not exists %s(
                code char(6) not null,
                name char(20))'''%HA300STOCKTABLE

        cur.execute(sql)

    hs300url = 'ftp://115.29.204.48/webdata/000300cons.xls'
    resp, cont = h.request(szstockurl, headers=header)
    if resp.status == 200:
        try:
            f = open(os.path.join('temp', 'HS300.xls'), 'wb')
        except:
            print('can not write HS300 stocks to file! ')
        f.write(cont)
        f.close()


if __name__=='__main__':

    myConn = pymysql.connect(host='127.0.0.1', user='root', password='worship',charset='utf8')
    initialDatabase(myConn)
    initialKlineTable(myConn)
    initialStockTable(myConn)
