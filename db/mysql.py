#!/usr/bin/python
# coding=utf-8

import MySQLdb
import re
import sys


class sqlMgr:
    '''
    mysql 数据库管理模块
    '''

    __KTableList =[]
    

    def __init__(self, ipAddr, user, passwd, db_name):
        self.ipAddr = ipAddr
        self.user = user
        self.passwd = passwd
        self.db_name = db_name

        self.db = MySQLdb.connect(ipAddr, user, passwd, db_name, charset="utf8")
        self.cursor = self.db.cursor()


        tableDB = MySQLdb.connect(ipAddr, user, passwd, "information_schema", charset="utf8")
        tableCursor = tableDB.cursor()

        SQL = " select TABLE_NAME from TABLES where ( TABLE_SCHEMA = '" + db_name + "' ) "
        try:  
            tableCursor.execute(SQL)
            results = tableCursor.fetchall()
            for index in results:
                tableName = index[0]
                self.__KTableList.append(str(tableName))
        except:
            # Rollback in case there is any error
            print "read tableName error " 

        tableDB.close()
         
        #print self.__KTableList
        

    def __del__(self):
        self.db.close()


    def queryAllCode(self):

        codeList = []

        SQL = u"select 代码 from basics where ( 代码 > '000000' ) "
        #SQL = u"select 代码 from basics"
        SQL.encode('utf-8')


        try:  
            self.cursor.execute(SQL)
            #print 'cp1'
            results = self.cursor.fetchall()
            #print 'cp2'
            for index in results:
                tableName = index[0]
                codeList.append(str(tableName))
        except:
            # Rollback in case there is any error
            print "read tableName error "

        return codeList  
    
    def test(self):
        '''
        test function work
        '''       
        cursor = self.cursor
        cursor.execute("SELECT VERSION()")
        data = cursor.fetchone()
        print "Database version : %s " % data


    def insertBasicTable(self, data):
        '''
        股票列表
        '''
        self.__inserOptComm(data, 16, 'basics')

    def insertMainReportTable(self, data):
        '''
        业绩报告（主表）
        '''
        self.__inserOptComm(data, 11, 'mainreport')

        
    def insertProfitTable(self, data):
        '''
        盈利能力
        '''
        self.__inserOptComm(data, 10, 'profit')
        
    def insertGrowthTable(self, data):
        '''
        成长能力
        '''
        self.__inserOptComm(data, 9, 'growth') 


    def insertOperationTable(self, data):
        '''
        运营能力
        '''
        self.__inserOptComm(data, 9, 'operation')    

    def insertDebtpayingTable(self, data):
        '''
        偿债能力
        '''
        self.__inserOptComm(data, 9, 'debtpaying')

    def insertCashflowTable(self, data):
        '''
        现金流量
        '''
        self.__inserOptComm(data, 8, 'cashflow')

    def insertKLine(self, data, tableName):
        '''
        插入K线
        '''
        self.__inserOptComm(data, 15, tableName)



    def create_K_Table_yahoo(self, tableName):

        if tableName in self.__KTableList:
            return        

        SQL = "CREATE TABLE " + tableName + " ( "
        SQL += " 日期 datetime PRIMARY KEY, "
        SQL += " 开盘价 nVarChar(10) , " 
        SQL += " 最高价 nVarChar(10) , "
        SQL += " 最低价 nVarChar(10) , "
        SQL += " 收盘价 nVarChar(10) , "
        SQL += " 成交量 nVarChar(20) , "
        SQL += " 前复权价 nVarChar(10)"
        SQL += " ) "
        try:  
            self.cursor.execute(SQL)
            self.db.commit()
            self.__KTableList.extend(tableName)
        except:
            # Rollback in case there is any error
            print "create_K_Table error  tableName = " + tableName



    def create_K_Table(self, tableName):

        if tableName in self.__KTableList:
            return        


        SQL = "CREATE TABLE " + tableName + " ( "
        SQL += " 日期 datetime PRIMARY KEY, "
        SQL += " 开盘价 nVarChar(10) , " 
        SQL += " 最高价 nVarChar(10) , "
        SQL += " 收盘价 nVarChar(10) , "
        SQL += " 最低价 nVarChar(10) , "
        SQL += " 成交量 nVarChar(20) , "
        SQL += " 价格变动 nVarChar(10) , "
        SQL += " 涨跌幅 nVarChar(10) , "
        SQL += " 5日均价 nVarChar(10) , "
        SQL += " 10日均价 nVarChar(10) , "
        SQL += " 20日均价 nVarChar(10) , "
        SQL += " 5日均量 nVarChar(20) , "
        SQL += " 10日均量 nVarChar(20) , "
        SQL += " 20日均量 nVarChar(20) , "
        SQL += " 换手率 nVarChar(10) "
        SQL += " ) "
        try:  
            self.cursor.execute(SQL)
            self.db.commit()
            self.__KTableList.extend(tableName)
        except:
            # Rollback in case there is any error
            print "create_K_Table error  tableName = " + tableName

    def insertKLine_yahoo(self, data, tableName):
        '''
        插入K线
        '''
        self.__inserOptComm(data, 7, tableName)        

    def __inserOptComm(self, data, size, tableName):
        
        '''
        数据库插入通用函数
        '''

        model = "(.*)"
        for index in range(1, size):
            model += ",(.*)"



        pattern = re.compile(model)
        try: 
            res = pattern.search(data).groups()
        except:
            # Rollback in case there is any error
            print "data error "
            return

        
        insertValue = "("
        dataSize = len(res)
        loopSize = dataSize - 1
        
        for num in range(0,loopSize):
            insertValue += "'" + res[num] + "'" +","
        insertValue += "'" + res[loopSize] + "'"
        insertValue += ")"

        inserSQL = "INSERT INTO "
        inserSQL += tableName
        inserSQL += " VALUES "
        inserSQL += insertValue 
        
        try:  
            self.cursor.execute(inserSQL)
            self.db.commit()
        except:
            # Rollback in case there is any error
            print "error :" + data 
            self.db.rollback()
