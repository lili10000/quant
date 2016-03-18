# coding=utf-8 
#!/usr/bin/python

from db.mysql import sqlMgr
import tushare as ts
import re
import csv
import os

class tushareBase:

    __columnList = ['c1','c2','c3','c4','c5','c6','c7','c8','c9','c10','c11','c12']
    __filename = 'tmp.csv'


    def __init__(self):
        
        self.sql = sqlMgr('localhost', 'root', '123456', 'stockdb')
        
    def getStockBasics(self):
        '''
        获取股票基本面数据
        '''
        print 'getStockBasics work'
        
        df = ts.get_stock_basics()

        print 'download data ok'
        
        for codeIndex in range(0, len(df.index)):
            data = str(df.index[codeIndex])
            for rowsIndex in range(0, len(df.columns)):
                data += "," + str(df.ix[codeIndex, rowsIndex])
            self.sql.insertBasicTable(data)
            
        print 'get StockBasics data ok'

    def getMainReport(self, year, season):
        '''
        获取股票业绩报告（主表）
        '''
        print 'getMainReport work'        
         
        df = ts.get_report_data(year, season)
        df.to_csv(self.__filename)
     
        self.__getDataComm(12, year, "MainReport")


    def getProfit(self, year, season):
        '''
        获取股票盈利能力数据
        '''
        print 'getProfit work' 
        
        df = ts.get_profit_data(year, season)
        df.to_csv(self.__filename)
        
        self.__getDataCommSeason(10, year, season, "Profit")
        
    def getGrowth(self, year, season):
        '''
        获取股票成长能力数据  ---待测试
        '''
        print 'getGrowth work'
        
        df = ts.get_growth_data(year, season)
        df.to_csv(self.__filename)
        
        self.__getDataCommSeason(9, year, season, "Growth")

    def getOperation(self, year, season):
        '''
        获取股票运营能力数据
        '''
        print 'getOperation work'
         
        df = ts.get_operation_data(year, season)
        df.to_csv(self.__filename)
               
        self.__getDataCommSeason(9, year, season, "Operation")

    def getDebtpaying(self, year, season):
        '''
        获取股票运营能力数据
        '''
        print 'getDebtpaying work'

        df = ts.get_debtpaying_data(year, season)
        df.to_csv(self.__filename)
     
        self.__getDataCommSeason(9, year, season, "Debtpaying")
        
    def getCashflow(self, year, season):
        '''
        获取股票运营能力数据
        '''
        print 'getCashflow work'

        df = ts.get_cashflow_data(year, season)
        df.to_csv(self.__filename)
     
        self.__getDataCommSeason(8, year, season, "Cashflow")    

    def __getDataComm(self, size, year, dataName):

        print ''

        listIndex = size - 1
           
        file = open(self.__filename)
        file.readline()
        reader = csv.reader(file)
        
        for self.__columnList in reader:
            data = ""
            list = self.__columnList[1:listIndex]
            for index in list:
                data += index + ","
            data += str(year) + "-" + self.__columnList[listIndex]
            self.sql.insertMainReportTable(data)

        print "get " + dataName +" OK"
        file.close()
        os.remove(self.__filename)



    def __Debtpaying(self, data):  
        self.sql.insertDebtpayingTable(data)

    def __Operation(self, data):  
        self.sql.insertOperationTable(data)

    def __Growth(self, data):  
        self.sql.insertGrowthTable(data)

    def __Profit(self, data):  
        self.sql.insertProfitTable(data)

    def __Cashflow(self, data):  
        self.sql.insertCashflowTable(data)

    __operator = {'Debtpaying':__Debtpaying, \
                  'Operation':__Operation, \
                  'Growth':__Growth, \
                  'Profit':__Profit, \
                  'Cashflow':__Cashflow}

    def __getDataCommSeason(self, size, year, season, dataName):

        print ''         
        file = open(self.__filename)
        file.readline()
        reader = csv.reader(file)
        
        for self.__columnList in reader:
            data = ""
            list = self.__columnList[1:size]
            for index in list:
                data += index + ","
            data += str(year) + "-" + str(season*3) + "-30"
            self.__operator.get(dataName)(self, data)

        print "get " + dataName +" OK"
        file.close()
        os.remove(self.__filename)
        
        
    def test(self):
        '''
        test function work
        '''       
        self.sql.test()
