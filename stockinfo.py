#!/usr/bin/python3
# coding=utf-8
"""
该文件用于下载上海证券交易所和深证证券交易所中的股票基本信息，也下载HS300的基本信息
"""

import pymysql
import httplib2
import os
from bs4 import BeautifulSoup
from ftplib import FTP

DOWNLOADPATH = 'temp'
SZSTOCKFILE = 'SZstockInfo.txt'
SHSTOCKFILE = 'SHstockInfo.txt'
HS300FILE = 'HS300Infor.txt'

def getSHStockInfo(path = DOWNLOADPATH):
    shhost = 'http://www.sse.com.cn/assortment/stock/list/share/'
    shstockurl = 'http://query.sse.com.cn/security/stock/downloadStockListFile.do?csrcCode=&stockCode=&areaName=&stockType=1'
    h = httplib2.Http(path)
    header = {'user-agent': \
                  'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.152 Safari/537.36'}

    resp, cont = h.request(shhost, headers=header)

    if resp.status == 200:
        try:
            f = open(os.path.join('temp', 'shstock.xls'), 'wb')
        except:
            print('can not write SH stocks to file! ')
        f.write(cont)
        f.close()


def getSZStockInfo(path = DOWNLOADPATH):
    szstockurl = 'http://www.szse.cn/szseWeb/ShowReport.szse?SHOWTYPE=EXCEL&CATALOGID=1110&tab1PAGENUM=1&ENCODE=1&TABKEY=tab1'
    h = httplib2.Http(path)
    header = {'user-agent': \
                  'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.152 Safari/537.36'}


    soup = BeautifulSoup(open(os.path.join('temp', 'szstock.html'), 'rb'), 'html.parser')
    content = soup.find_all('td', class_='cls-data-td')
    res = []
    line = []
    try:
        fh = open(os.path.join(DOWNLOADPATH,SZSTOCKFILE),'w',encoding='GBK')
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
    hs300url = 'ftp://115.29.204.48/webdata/000300cons.xls'
    h = httplib2.Http(path)
    header = {'user-agent': \
                  'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.152 Safari/537.36'}

    resp, cont = h.request(hs300url, headers=header)
    if resp.status == 200:
        try:
            f = open(os.path.join('temp', 'HS300.xls'), 'wb')
        except:
            print('can not write HS300 stocks to file! ')
        f.write(cont)
        f.close()

# def parseSHStockInfo(path=DOWNLOADPATH):
#     pass;
#
# def parseSZStockInfo(path=DOWNLOADPATH):
#     pass;
#

if __name__ == '__main__':
    # getSZStockInfo()
    # getSHStockInfo()
    getHS300Infor()