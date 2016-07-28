# coding=utf-8

from sqlalchemy import create_engine
from db.mysql import sqlMgr
import tushare as ts

class tushareKLine:
   
    
    def __init__(self):    
        self.engine = create_engine('mysql://root:861217@127.0.0.1/stockdb?charset=utf8')#存入数据库
        self.sql = sqlMgr('localhost', 'root', '861217', 'stockdb')
        
    def getHistKData(self, code, start, end):
        '''
        历史K线
        '''
        #print 'getHistKData work'

        if len(start) == 0 or len(end) == 0 :   
            df = ts.get_h_data(code, autype='hfq')
        
        else :
            df = ts.get_h_data(code, start, end, autype='hfq')
            
        if df is None:
            return
        
        ktype = 'D'
        tableName = 'k_' + ktype + "_" + code
        
        df.to_sql(tableName, self.engine, if_exists='append')
 

    def getAllHistKData(self):
        ktype = 'D'
        codeList = []
        codeTmp = self.sql.queryAllCode()
        for index in codeTmp:
            tableName = index
            print '\n\nstart do ' + tableName + '\n'
            
            self.getHistKData(str(index), '1990-01-01', '1993-01-01')
            self.getHistKData(str(index), '1993-01-01', '1996-01-01')
            self.getHistKData(str(index), '1996-01-01', '1999-01-01')
            self.getHistKData(str(index), '1999-01-01', '2002-01-01')
            self.getHistKData(str(index), '2002-01-01', '2005-01-01')
            self.getHistKData(str(index), '2005-01-01', '2008-01-01')
            self.getHistKData(str(index), '2008-01-01', '2011-01-01')
            self.getHistKData(str(index), '2011-01-01', '2014-01-01')
            self.getHistKData(str(index), '2014-01-01', '2017-01-01')


