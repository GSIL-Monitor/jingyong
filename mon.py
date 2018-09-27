import pymongo
import pandas as pd
from sqlalchemy import create_engine
from duoyuan_conn import *
import datetime



yesday=datetime.datetime.now().date()-datetime.timedelta(days=1)
dcs=dcs()

myclient = pymongo.MongoClient('139.224.144.241',27017)
mydb = myclient.pandora
mydb.authenticate("readonly","Jingyong123")
tab=mydb.app_info

sql="""
SELECT app_no,seq_no from pandora_model_history WHERE seq_no IS NOT NULL AND date(create_time)=DATE_SUB(CURDATE(),INTERVAL 1 day) 
"""

data=pd.read_sql_query(sql,dcs)

ids=data['seq_no'].tolist()



result=mydb.parses.find({'_id':{"$in":ids}})
finall=[]

for i in result:
    print(i)
    report = i['report']
    report['seq_no'] = i['_id']
    seq_data=pd.DataFrame(report,index=[0])
    finall.append(seq_data)
    print(seq_data)
try:
    data2=pd.concat(finall)
    data3 = pd.merge(data, data2, on='seq_no', how='inner')
    data3.drop(['pred','score'],inplace=True,axis=1)
    data3['create_time']=yesday
    data3.to_sql('dd_model_score', ddw(), index=False, if_exists='append')
except ValueError as e:
    print(e)



print(data3)