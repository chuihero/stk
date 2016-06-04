#!/usr/bin/python3
# coding=utf-8
"""
该文件用于下载上海证券交易所和深证证券交易所中的股票基本信息，也下载HS300的基本信息
"""

import os
from bs4 import BeautifulSoup
import xlrd
from itertools import islice
import datetime
from easyutils import timeutils

DOWNLOADPATH = 'config'
SZSTOCKFILE = '上市公司列表.xls'
SHSTOCKFILE = 'A股.xls'
HS300FILE = '000300cons.xls'

def getSHStockInfo(path = DOWNLOADPATH):
    filepath = os.path.join(path, SHSTOCKFILE)
    assert os.path.exists(filepath), \
            '没有找到沪市股票文件，请从“http://www.sse.com.cn/assortment/stock/list/share/”下载，并放置到{}下'.format(path)

    res = []
    fh = open(filepath,'r',encoding='gbk')
    for line in islice(fh,1,None):
        res.append(line.split())

    return res


def getSZStockInfo(path = DOWNLOADPATH):
    filepath = os.path.join(path, SZSTOCKFILE)
    assert os.path.exists(filepath), \
        '没有找到深市股票文件，请从“http://www.szse.cn/szseWeb/ShowReport.szse?SHOWTYPE=EXCEL&CATALOGID=1110&\
        tab1PAGENUM=1&ENCODE=1&TABKEY=tab1”下载，并放置到{}下'.format(path)

    soup = BeautifulSoup(open(filepath, 'rb'), 'html.parser')
    content = soup.find_all('td', class_='cls-data-td')
    res = []
    line = []

    for i in range(len(content)):
        if i%20==0 and i>0:
            res.append(line)
            line = []
        text = content[i].get_text()
        line.append(text)
    res.append(line)
    return res



def getHS300Infor(path = DOWNLOADPATH):
    filepath = os.path.join(path,HS300FILE)
    assert os.path.exists(filepath),\
            '没有找到沪深300文件，请从“ftp://115.29.204.48/webdata/000300cons.xls”下载，并放置到{}下'.format(path)

    fh = xlrd.open_workbook(filepath)
    data = fh.sheet_by_index(0)
    res = data.col_values(0)

    return res


def needUpdate(latestDate):
    #判断从最新日期到今日，是否需要更新数据。如果是周六周日，或者假期，就不再更新数据
    assert type(latestDate) == datetime.date, '日期类型错误，请检查'
    today = datetime.date.today()
    one = datetime.timedelta(1)

    NeedUpdate = False
    date = latestDate + one
    while date <= today:
        if date.weekday() not in [5,6]:
            NeedUpdate = True
            return NeedUpdate

        date = date + one
    return NeedUpdate


if __name__ == '__main__':
    # res = getSZStockInfo()
    # getSHStockInfo()
    # getHS300Infor()
    # print(res)
    date = datetime.date(2016,6,3)
    print(needUpdate(date))