import pandas as pd
from sqlalchemy import create_engine

cc=create_engine('mysql+pymysql://dw_dev:hydw@2018@10.253.169.47/ccms2?charset=utf8')
cc=cc.connect()
fq=create_engine('mysql+pymysql://dw_dev:hydw@2018@10.253.169.47/fqmall_ht_prod?charset=utf8')
fq=fq.connect()

cre_sql="""SELECT DATE(a.create_date) create_time,COUNT(a.app_application_no) num1,sum(a.APPLY_AMOUNT) app_amount,c.custfrom_name from d_application_pay a
LEFT JOIN cust_customer b
on a.app_user_identityno=b.identity_no
LEFT JOIN mis_dic_custfrom c
ON b.CUST_FROM=c.custfrom_code
WHERE DATE(a.create_date) >'2017-09-01' AND DATE(a.create_date) < DATE(NOW()) 
GROUP BY DATE(a.create_date),c.custfrom_name"""

pass_sql="""SELECT DATE(a.approval_pro_end_date) pass_time,COUNT(a.app_application_no) num2,sum(a.APPLY_AMOUNT) app_amount,c.custfrom_name from d_application_pay a
LEFT JOIN cust_customer b
on a.app_user_identityno=b.identity_no
LEFT JOIN mis_dic_custfrom c
ON b.CUST_FROM=c.custfrom_code
WHERE DATE(a.approval_pro_end_date) >'2017-09-01'AND DATE(a.approval_pro_end_date) < DATE(NOW()) and a.faceResult='pass'
GROUP BY DATE(a.approval_pro_end_date),c.custfrom_name"""


cre_data=pd.read_sql(cre_sql,fq)
pass_data=pd.read_sql(pass_sql,fq)
data=pd.merge(cre_data,pass_data,how='outer',left_on=['create_time','custfrom_name'],right_on=['pass_time','custfrom_name'])
data.loc[data['create_time'].isnull(),'create_time']=data['pass_time']
data.loc[data['pass_time'].isnull(),'pass_time']=data['create_time']
data.columns=['create_time','app_num','app_amount','cust_from','pass_time','pass_num','pass_amount']
print('tooooooooooooooooooooooooooooooooooo')
data.to_sql('cust_from',fq,index=False,if_exists='replace')


print(data.head())
# data= pd.read_csv('e:\\22.csv',header=None,names=['phone_no','lost_date'])
# data.to_sql('lost_phone',cc,if_exists='replace',index=False)
# print(data.head())
