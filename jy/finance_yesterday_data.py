#! /root/anaconda3/bin/python3

import pandas as pd
from sqlalchemy import create_engine
import re
import yagmail
import glob
import os

conn = create_engine('mysql+pymysql://atbi_rw_liangqs:Ric2jk-Ks749rxIIRgu08@rdskkv2o1o24yiun470v.mysql.rds.aliyuncs.com/bi?charset=utf8')

sender='财务昨日数据'
receiver=['feng.yang@atzuche.com','settle@atzuche.com','account@atzuche.com','xiaodong.tang@atzuche.com','dandan.zu@atzuche.cn','myyang@atzuche.com','lina.zhou@atzuche.cn','liang.cao@atzuche.cn']
#receiver=['liang.cao@atzuche.cn','lina.zhou@atzuche.cn']
projectname='/home/caoliang/five_xlsx'


sqls=['SELECT * from wallet_log where TIMESTAMPDIFF(day,DATE(create_time),CURDATE())=1',
     'SELECT * from wed_trans_pay where TIMESTAMPDIFF(day,DATE(trans_time),CURDATE())=1',
     'SELECT * from trans_pay_offline where TIMESTAMPDIFF(day,DATE(trans_time),CURDATE())=1',
     'SELECT * from settle_log where TIMESTAMPDIFF(day,DATE(create_time),CURDATE())=1',
     'SELECT * from trans_pay where TIMESTAMPDIFF(day,DATE(trans_time),CURDATE())=1'
]
if not os.path.exists('%s'%projectname):
    os.mkdir('%s'%projectname)
    for sql in sqls:
        name= re.findall('.*from (.*?) where',sql)[0]
        # names.append(name)
        print(name)
        data= pd.read_sql_query(sql,conn)
        data.to_excel('%s/%s.xlsx'%(projectname,name),index=False)
else:
    for sql in sqls:
        name= re.findall('.*from (.*?) where',sql)[0]
        # names.append(name)
        print(name)
        data= pd.read_sql_query(sql,conn)
        data.to_excel('%s/%s.xlsx'%(projectname,name),index=False)

content=glob.glob('%s/*.xlsx'%projectname)

yag = yagmail.SMTP(user="bi@atzuche.com", password="BIatzc2017", host="smtp.qiye.163.com", port="465")
yag.send(receiver,sender,contents=content)
yag.close()
print('send success')

