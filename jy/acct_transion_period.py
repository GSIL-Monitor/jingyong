import pandas as pd
from conn import *
import datetime
import time

# s1=time.strftime('%Y-%m-%d',datetime.datetime.s(datetime.datetime.now().date()))

s0=(datetime.datetime.now().date()-datetime.timedelta(days=1)).strftime('%Y-%m-%d')+' 05:00:00'
s1=datetime.datetime.now().date().strftime('%Y-%m-%d')+' 05:00:00'
print(s0,s1)

data=pd.read_sql_query("select * from acct_transaction_period_detail where create_time > '{}' and create_time<='{}'".format(s0,s1),fq())

types=data[data['PAY_TYPE']==0].index
data['PAY_BACK_DATE']=data.loc[types,'CREATE_TIME']-datetime.timedelta(days=1)#
nulls=data['PAY_BACK_DATE'].isnull()
data.loc[nulls,'PAY_BACK_DATE']=data.loc[nulls,'CREATE_TIME']
data.drop('ID',axis=1,inplace=True)
print(len(data))
data.to_sql('pay_back_detail',dw(),index=False,if_exists='append')
