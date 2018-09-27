import pandas as pd
from sqlalchemy import create_engine

cc=create_engine('mysql+pymysql://dw_dev:hydw@2018@10.253.169.47/ccms2?charset=utf8')
cc=cc.connect()
fq=create_engine('mysql+pymysql://dw_dev:hydw@2018@10.253.169.47/fqmall_ht_prod?charset=utf8')
fq=fq.connect()

sqls="""SELECT a.APPLICATION_ID,b.borrow_money_use
from loan_loan a
LEFT JOIN d_input_app b
on a.APPLICATION_ID=b.applicationNo
WHERE DATE(create_time)>'2018-03-01'"""

sqlss="""SELECT hy_application_no,inv_username
from inv_application_pay
WHERE
(inv_username=10002 AND `status` in (2,3,4,5)) OR 
(inv_username=10003 AND `status` IN (2,3,4)) OR
(inv_username=10010 AND `status` in (2,3))
UNION
SELECT app_No as hy_application_no,inv_username from cust_tocash a 
INNER JOIN inv_hy_application b
on a.app_No=b.hy_application_no
WHERE TC_STATUS ='APRY'"""

s1=pd.read_sql(sqls,fq)
ss1=pd.read_sql(sqlss,fq)
s1['APPLICATION_ID']=s1['APPLICATION_ID'].astype(str)
s2=pd.merge(s1,ss1,how='inner',left_on='APPLICATION_ID',right_on='hy_application_no')
sql3="""select DIC_NAME,DIC_CODE from d_dit_dic"""
data3=pd.read_sql(sql3,fq)
data5=pd.merge(s2,data3,left_on='borrow_money_use',right_on='DIC_CODE',how='left').drop(['DIC_CODE','hy_application_no'],axis=1)
isnull=data5['DIC_NAME'].isnull()
null_data=data5.loc[isnull,'DIC_NAME']
data5.loc[isnull,'DIC_NAME']=data5['borrow_money_use']
ss=pd.pivot_table(data5,values='APPLICATION_ID',index='inv_username',columns='DIC_NAME',aggfunc='count')
ssr=ss.reset_index(level=0)
ssr.to_excel('e:\\机构贷款用途汇总分析2.xlsx')

print(data5)