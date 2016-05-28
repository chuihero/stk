#!/usr/bin/python3
# coding=utf-8
# author:chui

import os
import httplib2
import threading
import queue
import re
import datetime
import pandas as pd
from pandas import DataFrame,Series,Timestamp
# import json
# import csv

MAXDOWNLOADTHRD = 6
MAXRETRYTIMES = 4
STOCKDATAHISTORY = 200 #股票历史数据时间
STOCKSAVEPATH =  r'History Data'


def getStockFileList(stockRange):
    '''
    input:
        stockRange:'HS300','HSA'
    output:
        codelist,
    读取配置文件的股票列表，并返回股票codelist
    '''
    assert stockRange in ('HSA','HS300'),\
            '输入有误，请检查是否是“HSA”，“HS300”'

    fileDir = 'config' 
    fileDict = {'HSA':'沪深A股.txt',\
                'HS300':'沪深300成分股.txt'}

    stockFile = os.path.join(fileDir,fileDict[stockRange])
    stockCodeList = []
    stockNameList = []
    stockBusiList = []

    with open(stockFile,encoding='GBK') as reader:
        for line in reader:
            data = line.split('\t')
            stockCodeList.append(data[0][2:])
            stockNameList.append(data[1])
            stockBusiList.append(data[2])
            
    return (stockCodeList,stockNameList,stockBusiList)

def parseSinaStockDayDate(content,stockCode,scale):
    """
    把新浪股票日线数据解析成csv文件或其他格式数据
    :param content: 新浪返回的日线数据请求response的content
    :return:
    """
    par = {'date': '{day:"(\S*? \S*?)",', \
           'open': 'open:"(\S*?)",', \
           'close': 'close:"(\S*?)",', \
           'high': 'high:"(\S*?)",', \
           'low': 'low:"(\S*?)",', \
           'vol': 'volume:"(\S*?)"'}


    cont = content.decode('utf-8')

    # strPar = re.compile(par['date'])

    date = re.findall(re.compile(par['date']), cont)

    openprice = re.findall(re.compile(par['open']), cont)
    close = re.findall(re.compile(par['close']), cont)
    high = re.findall(re.compile(par['high']), cont)
    low = re.findall(re.compile(par['low']), cont)
    vol = re.findall(re.compile(par['vol']), cont)

    if not os.path.exists(STOCKSAVEPATH):
        os.mkdir(STOCKSAVEPATH)
        
    fileName = os.path.join(STOCKSAVEPATH, (stockCode) + '_' + scale + '.csv')

    # f = open(fileName,'w',newline='')
    # csvwriter = csv.writer(f)
    # csvwriter.writerow(['date','open','high','low','close','vol'])
    # for i in range(len(date)):
    #     csvwriter.writerow([date[i],openprice[i],high[i],])

    df = DataFrame(index=date)
    df['开盘价'] = openprice
    df['最高价'] = high
    df['最低价'] = low
    df['收盘价'] = close
    df['成交量'] = vol
    df.index.name = '日期'
    df.to_csv(fileName, encoding='gbk')


def downLoadFile(stockList, destDir=STOCKSAVEPATH, dataType='Day'):
    """
    根据股票列表，把股票数据下载到目标文件夹中去
    :param stockList:
    :param destDir:
    :return:
    """
    assert dataType in ('Day','30Min')

    global STOCKDATAHISTORY,MAXRETRYTIMES

    if not os.path.exists(destDir):
        os.mkdir(destDir)

    stockQueue = queue.Queue()
    sLock = threading.Lock()

    for code in stockList:
        stockQueue.put(code)

    class downThread(threading.Thread):
        def __init__(self,tid,sQueue,directory = destDir,dataType = dataType):
            super(downThread, self).__init__()
            self.id = tid
            self.sQueue = sQueue
            self.directory = directory
            self.dataType = dataType

        def run(self):

            global STOCKDATAHISTORY
            startDate = datetime.date.today()
            endDate = startDate - datetime.timedelta(STOCKDATAHISTORY)  # 这里的日期是自然日，而下载的日期均为工作日，因此，需要留有一定余量
            ##    WEBADDRPRE = "http://table.finance.yahoo.com/table.csv?s="
            beginDateStr = ''.join(endDate.isoformat().split('-'))
            endDateStr = ''.join(startDate.isoformat().split('-'))

            while not self.sQueue.empty():
                sLock.acquire()
                code = self.sQueue.get()
                sLock.release()

                if code[0] == '6':
                    codeMarket = '0'
                else:
                    codeMarket = '1'
                symbol = codeMarket + (code)

                if self.dataType == 'Day':
                    url = 'http://quotes.money.163.com/service/chddata.html?code=' + symbol+ '&start='+ beginDateStr+'&end='+endDateStr + '&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;TURNOVER;VOTURNOVER;VATURNOVER;TCAP;MCAP'

                    h = httplib2.Http('.cache')
                    # url = 'http://vip.stock.finance.sina.com.cn/quotes_service/view/CN_BillList.php?sort=ticktime&symbol=%s&num=11' \
                    #       % (symble)
                    # h = httplib2.Http('.daySinacache')
                    header = {'user-agent': \
                                  'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.152 Safari/537.36'}

                elif self.dataType == '30Min':
                    url = ''

                tryed = MAXRETRYTIMES
                while tryed>0:
                    resp, content = h.request(url, headers=header)
                    if resp.status != 200:
                        tryed -=1
                        continue
                    else:
                        break

                if resp.status == 200:
                    # download successfully
                    # need parse the resp
                    # parseSinaStockDayDate(content,code,self.dataType)
                    fileSaveName = os.path.join(STOCKSAVEPATH, code+'.csv')
                    try:
                        f = open(fileSaveName, 'wb')
                    except:
                        print('文件写入失败')
                        return PROGRESS_FAIL
                    f.write(content)
                    f.close()

                else:

                    print('股票:%s 无法下载，请检查！'%self.code)


                #完成一次下载，继续下个下载，直到队列为空
                continue
    #class end
    thrds = []
    for i in range(MAXDOWNLOADTHRD):
        thd = downThread(i,stockQueue)
        thrds.append(thd)
        thd.start()

    for t in thrds:
        t.join()
    print('所有股票下载完成!')






    
if __name__ =='__main__':
    (a,b,c) = getStockFileList('HSA')
    downLoadFile(a)
