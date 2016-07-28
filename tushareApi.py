# coding=utf-8

from sqlalchemy import create_engine
from db.mysql import sqlMgr
import tushare as ts

class tushareKLine:
   
    
    def __init__(self):    
        self.engine = create_engine('mysql://root:861217@127.0.0.1/stockdb?charset=utf8')#存入数据库
        self.sql = sqlMgr('localhost', 'root', '861217', 'stockdb')
        
    def getHistKData(self, code, start, end, ktype):
        '''
        历史K线
        '''
        print 'getHistKData work'

        if len(start) == 0 or len(end) == 0 :   
            df = ts.get_h_data(code, autype='hfq')
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

        tableName = 'k_' + ktype + "_" + code 
        df.to_sql(tableName, self.engine)

    def getAllHistKData(self):
        ktype = 'D'
        codeList = []
        codeTmp = self.sql.queryAllCode()
        for index in codeTmp:
            tableName = index
            self.getHistKData(str(index), '', '', 'D')



