import pandas as pd
from sqlalchemy import create_engine
import datetime
import time
import pymysql
real=datetime.datetime.now().date()
today=datetime.datetime.now().date()-datetime.timedelta(days=1)
conn3 = pymysql.connect(
    host="10.253.169.47",
    user="dw_dev",
    passwd="hydw@2018",
    charset="utf8",
    db='dw_base',
    use_unicode=False
)
cur3=conn3.cursor()

cc=create_engine('mysql+pymysql://dw_dev:hydw@2018@10.253.169.47/ccms2?charset=utf8')
cc=cc.connect()
fq=create_engine('mysql+pymysql://dw_dev:hydw@2018@10.253.169.47/fqmall_ht_prod?charset=utf8')
fq=fq.connect()
dw=create_engine('mysql+pymysql://dw_dev:hydw@2018@10.253.169.47/dw_base?charset=utf8')
dw=dw.connect()
test=create_engine('mysql+pymysql://coretest:Hengyuan@0326@10.253.3.69/test?charset=utf8')
test=test.connect()

sql1="""select * from sx_data"""
data1=pd.read_sql(sql1,fq)
data1.to_sql('sx_data_yes',index=False,if_exists='replace')

