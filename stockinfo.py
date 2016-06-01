#!/usr/bin/python3
# coding=utf-8
"""
该文件用于下载上海证券交易所和深证证券交易所中的股票基本信息，也下载HS300的基本信息
"""

import pymysql
import httplib2
import os
from bs4 import BeautifulSoup
import xlrd
from itertools import islice

DOWNLOADPATH = 'config'
SZSTOCKFILE = 'SZstockInfo.txt'
SHSTOCKFILE = 'A股.xls'
HS300FILE = '000300cons.xls'

def getSHStockInfo(path = DOWNLOADPATH):
    filepath = os.path.join(path,SHSTOCKFILE)
    assert os.path.exists(filepath),\
                          '没有找到沪市股票文件，请从“http://www.sse.com.cn/assortment/stock/list/share/”下载，并放置到{}下'.format(path)

    res = []
    fh = open(filepath,'r',encoding='gbk')
    for line in islice(fh,1,None):
        res.append(line.split())

    return res


def getSZStockInfo(path = DOWNLOADPATH):
    szstockurl = 'http://www.szse.cn/szseWeb/ShowReport.szse?SHOWTYPE=EXCEL&CATALOGID=1110&tab1PAGENUM=1&ENCODE=1&TABKEY=tab1'
    h = httplib2.Http(path)
    header = {'user-agent': \
                  'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.152 Safari/537.36'}


    soup = BeautifulSoup(open(os.path.join(path, 'szstock.html'), 'rb'), 'html.parser')
    content = soup.find_all('td', class_='cls-data-td')
    res = []
    line = []
    try:
        fh = open(os.path.join(path,SZSTOCKFILE),'w',encoding='GBK')
    except:
        print('无法写入股票基本信息文件')
        return

    for i in range(len(content)):
        if i%20==0 and i>0 :
            line = []
            res.append(line)
            fh.write('\n')
        text = content[i].get_text()
        line.append(text)
        fh.write(text)
        fh.write('\t')
    fh.close()
    return res



def getHS300Infor(path = DOWNLOADPATH):
    filepath = os.path.join(path,HS300FILE)
    assert os.path.exists(filepath),\
            '没有找到沪深300文件，请从“ftp://115.29.204.48/webdata/000300cons.xls”下载，并放置到{}下'.format(path)

    fh = xlrd.open_workbook(filepath)
    data = fh.sheet_by_index(0)
    nrows = data.nrows
    res = []
    for line in range(1,nrows):
        res.append(data.row_values(line))

    return res

if __name__ == '__main__':
    # getSZStockInfo()
    # getSHStockInfo()
    # getHS300Infor()