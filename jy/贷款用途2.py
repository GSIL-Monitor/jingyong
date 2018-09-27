import pandas as pd
from sqlalchemy import create_engine

cc=create_engine('mysql+pymysql://dw_dev:hydw@2018@10.253.169.47/ccms2?charset=utf8')
cc=cc.connect()
fq=create_engine('mysql+pymysql://dw_dev:hydw@2018@10.253.169.47/fqmall_ht_prod?charset=utf8')
fq=fq.connect()

sqls="""SELECT hy_application_no,inv_username,loan_purpose
from inv_application_pay
WHERE
((inv_username=10002 AND `status` in (2,3,4,5)) OR 
(inv_username=10003 AND `status` IN (2,3,4)) OR
(inv_username=10010 AND `status` in (2,3))) and  DATE(create_date)>'2018-03-01'
UNION
SELECT a.app_No as hy_application_no,b.inv_username,c.borrow_money_use from cust_tocash a 
INNER JOIN inv_hy_application b
on a.app_No=b.hy_application_no
LEFT JOIN d_input_app c
on a.app_No=c.applicationNo
WHERE TC_STATUS ='APRY' and b.create_date >'2018-03-01'"""

data1=pd.read_sql(sqls,fq)

sql3="""select DIC_NAME,DIC_CODE from d_dit_dic"""
data3=pd.read_sql(sql3,fq)

data5=pd.merge(data1,data3,left_on='loan_purpose',right_on='DIC_CODE',how='left').drop(['DIC_CODE'],axis=1)
isnull=data5['DIC_NAME'].isnull()
null_data=data5.loc[isnull,'DIC_NAME']
data5.loc[isnull,'DIC_NAME']=data5['loan_purpose']
ss=pd.pivot_table(data5,values='hy_application_no',index='inv_username',columns='DIC_NAME',aggfunc='count')
ssr=ss.reset_index(level=0)
ssr.to_excel('e:\\机构贷款用途汇总分析.xlsx')

print(data5)