#!/usr/bin/python3
# coding=utf-8

import pymysql

class client():
    DATABASE = 'stock'

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
