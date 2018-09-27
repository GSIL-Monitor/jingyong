import pandas as pd
from sqlalchemy import create_engine
import datetime
import time
import pymysql


conn3 = pymysql.connect(
    host="10.253.169.47",
    user="dw_dev",
    passwd="hydw@2018",
    charset="utf8",
    db='dw_base',
    use_unicode=False
)
cur3=conn3.cursor()

cur3.execute('TRUNCATE dw_base.fact_order_fir_ver ')
conn3.commit()

cc=create_engine('mysql+pymysql://dw_dev:hydw@2018@10.253.169.47/ccms2?charset=utf8')
cc=cc.connect()
fq=create_engine('mysql+pymysql://dw_dev:hydw@2018@10.253.169.47/fqmall_ht_prod?charset=utf8')
fq=fq.connect()
dw=create_engine('mysql+pymysql://dw_dev:hydw@2018@10.253.169.47/dw_base?charset=utf8')
dw=dw.connect()


sql1="""SELECT identity_no,userName,reasononegrade2,facresult,create_time,TEMP_APP_NO,adjust_black_name
from rule_ysout_engine """

sql2="""select TEMP_APPLICATION_NO,LOAN_PRODUCT from cust_tempapply_info"""

sql3="""select ID,industry_code from loan_product"""


data1=pd.read_sql(sql1,fq)
data2=pd.read_sql(sql2,fq)
data3=pd.read_sql(sql3,fq)

s1=pd.merge(data1,data2,left_on='TEMP_APP_NO',right_on='TEMP_APPLICATION_NO',how='left').drop('TEMP_APPLICATION_NO',axis=1)

s2=pd.merge(data3,s1,left_on='ID',right_on='LOAN_PRODUCT',how='right').drop(['ID','LOAN_PRODUCT'],axis=1)

s2.columns=['fv_hy_industry_code','fv_identity_no','fv_userName','fv_reasononegradetwo','fv_facresult','fv_create_time','fv_temp_app_no','fv_adjust_black_name']

s2.to_sql('fact_order_fir_ver',dw,index=False,if_exists='append')


cc.close()
fq.close()
dw.close()
cur3.close()
conn3.close()