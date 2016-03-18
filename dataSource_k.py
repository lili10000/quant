#!/usr/bin/python
# coding=utf-8

from db.mysql import sqlMgr
import tushare as ts
import re
import csv
import os
import urllib
import time

class tushareKLine:
    __columnList = ['c1','c2','c3','c4', \
                    'c5','c6','c7','c8', \
                    'c9','c10','c11','c12', \
                    'c13','c14','c16','c16']

    __columnList_yahoo = ['c1','c2','c3','c4','c5','c6','c7']
    
    __filename = 'temp.csv'


    def __init__(self):    
        self.sql = sqlMgr('localhost', 'root', '123456', 'stockdb')

    def getHistKData(self, code, start, end, ktype):
        '''
        历史K线
        '''
        print 'getHistKData work'

        if len(start) == 0 or len(end) == 0 :   
            df = ts.get_hist_data(code)
            ktype = 'D'

        elif len(start) == 0 :
            df = ts.get_hist_data(code, start, end)
            
        elif cmp(ktype, 'D') == 0 or \
             cmp(ktype, 'W') == 0 or \
             cmp(ktype, 'M') == 0 or \
             cmp(ktype, '5') == 0 or \
             cmp(ktype, '15') == 0 or \
             cmp(ktype, '30') == 0 or \
             cmp(ktype, '60') == 0 :

            df = ts.get_hist_data(code, start, end, ktype)
            
        else:
            print 'input error'
            return

        df.to_csv(self.__filename)

        
        self.__write2DB(ktype, code)

    '''
    def getAllHistKData(self):
        ktype = 'D'
        codeList = []
        codeTmp = self.sql.queryAllCode()
        for index in codeTmp:
            tableName = index
            codeList.append(str(tableName))

    
        for codeName in codeList :
            print 'get code = ' + codeName
            df = ts.get_hist_data(codeName)
            if df.empty :
                print 'can not get data'
            else :              
                df.to_csv(self.__filename)
                self.__write2DB(ktype, codeName)
    '''
    def getAllHistKData_yahoo(self):

        ktype = 'D'
        
        codeList = []

        codeTmp = self.sql.queryAllCode()
        for index in codeTmp:
            tableName = index
            codeList.append(str(tableName))

        for codeName in codeList :
  

            startYear = '2016'
            startMon = '02'
            startDay = '01'

            while self.__getDataFromNet(startYear, startMon, startDay, codeName) != 0 :
                print '__getDataFromNet try again'
                time.sleep(3)
                
            self.__write2DB_yahoo(ktype, codeName)

    def __getDataFromNet(self, startYear, startMon, startDay, codeName):
        url = 'http://table.finance.yahoo.com/table.csv?s='
        if codeName >= '600000' :
            url += codeName + '.ss'             
        else :
            url += codeName + '.sz'
        try:
            url += '&a='+startMon+'&b='+startDay+'&c='+startYear+'&g=d&ignore=.csv'
            print 'start get data, code = ' + codeName
            urllib.urlretrieve(url, self.__filename)
            return 0;
        except:
            # Rollback in case there is any error
            print 'get csv error'
            return 1;

    
                

    def __write2DB(self, ktype, code):
        
        file = open(self.__filename)
        file.readline()
        reader = csv.reader(file)

        tableName = 'k_' + ktype + "_" + code 
        self.sql.create_K_Table(tableName)
        
        for self.__columnList in reader:
            data = self.__columnList[0]
            list = self.__columnList[1:15]
            for index in list:
                data += "," + index
            self.sql.insertKLine(data, tableName)
           
        print "get " + code +" data OK"
        file.close()
        os.remove(self.__filename)


    def __write2DB_yahoo(self, ktype, code):
        
        file = open(self.__filename)
        #file.readline()
        reader = csv.reader(file)

        tableName = 'k_' + ktype + "_" + code 
        self.sql.create_K_Table_yahoo(tableName)

        for line in reader:
            if line[0] != 'Date':
                return
            print 'recv data OK'
            break

        for self.__columnList_yahoo in reader:
            data = self.__columnList_yahoo[0]         
            list = self.__columnList_yahoo[1:7]       
            for index in list:
                data += "," + index
            self.sql.insertKLine_yahoo(data, tableName)

        print "get " + code +" data OK"
        file.close()
        os.remove(self.__filename)

