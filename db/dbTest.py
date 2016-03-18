#!/usr/bin/python
# -*- coding: utf-8 -*-


import tushare as ts
import re

from mysql import sqlMgr






#data = "000001,平安银行,银行,深圳,7.55,1180405.5,1430867.63,257050800.0,0.0,369000.0,5932600.0,4.15,0.84,10.54,1.16,19910403"
#sql.insertBasicTable(data)

#sql.test()

year = 2014
season = 3


#unicode(SQL, "utf-8")



sql = sqlMgr('localhost', 'root', '861217', 'stockdb')

sql.queryAllCode()


#print (data.decode('utf-8')).encode('gb2312')
#sql.insertMainReportTable(data)


#print df

#re.findall(r"(.*)\Z")
#res = re.findall(r"(.*)\Z", df)
#res = pattern.search(df).groups()

#print res[1]
