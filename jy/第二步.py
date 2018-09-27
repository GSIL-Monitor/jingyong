import pandas as pd
from sqlalchemy import create_engine

cc=create_engine('mysql+pymysql://dw_dev:hydw@2018@10.253.169.47/ccms2?charset=utf8')
cc=cc.connect()
fq=create_engine('mysql+pymysql://dw_dev:hydw@2018@10.253.169.47/fqmall_ht_prod?charset=utf8')
fq=fq.connect()
dw=create_engine('mysql+pymysql://dw_dev:hydw@2018@10.253.169.47/dw_base?charset=utf8')
dw=dw.connect()

sql1="""

SELECT a.*,f.*,b.type FROM
(SELECT NAME,iden,mobileno from dw_base.lose WHERE cx_day='2018-06-05' ) a
INNER JOIN
(SELECT sum(-a.AMOUNT) amount,DATE(a.CREATE_TIME) hk_day,c.MOBILE,c.IDENTITY_NO
from acct_transaction a 
LEFT JOIN loan_loan b
on a.LOAN_ID=b.ID
LEFT JOIN cust_customer c
on b.USERNAME=c.USERNAME
WHERE a.tran_type='PABK' and (DATE(a.CREATE_TIME) BETWEEN  DATE_SUB('2018-06-05',INTERVAL 9 DAY) and '2018-06-05')
GROUP BY c.mobile,c.IDENTITY_NO,DATE(a.CREATE_TIME))f
on a.mobileno=f.mobile or a.iden=f.IDENTITY_NO
LEFT JOIN
(select DISTINCT cust_mobile,cust_certificate_no,CASE WHEN (sx_type is null and late_day >=4) then '失信' WHEN sx_type=1 then '诉讼' WHEN sx_type=2 THEN '仲裁'  END as 'type' 
from fqmall_ht_prod.sx_data) b
on a.mobileno=b.cust_mobile or a.iden=b.cust_certificate_no"""
data1=pd.read_sql(sql1,fq)
data2=data1[['hk_day','amount','type']]
data3=pd.pivot_table(data2,values='amount',index='hk_day',aggfunc=('sum','count'))
data4=pd.pivot_table(data2,columns='type',index=['hk_day'],aggfunc=('count'))



all_num=len(data1.index)
sx=len(data1[data1['type']=='失信'].index)
zc=len(data1[data1['type']=='仲裁'].index)
ss=len(data1[data1['type']=='诉讼'].index)

# M0=len(data1[data1['scena']=='M0'].index)
# M1=len(data1[data1['scena']=='M1'].index)
# M2=len(data1[data1['scena']=='M2'].index)
# M3=len(data1[data1['scena']=='M3'].index)
# M4=len(data1[data1['scena']=='M4'].index)
# M5=len(data1[data1['scena']=='M5'].index)
# M6=len(data1[data1['scena']=='M6'].index)
# M7=len(data1[data1['scena']=='M7+'].index)

data2=data1[~data1['type'].isnull()]
print(1)
# data1['mobile']=data1['mobile'].astype(str)
# s1=pd.merge(data1,data2,left_on='mobile',right_on='mobileno',how='left').drop(['mobile'],axis=1)
# s2=pd.merge(s1,data3,left_on='iden',right_on='IDENTITY_NO',how='left').drop(['iden'],axis=1)
#
# nulls=s2['tag'].isnull()
# s2.loc[nulls,'tag']=s2.loc[nulls,'tag2']
# s2.drop_duplicates(inplace=True)
# s3=s2.drop(['tag2'],axis=1)
# print(s2)
#
# s3.to_sql('lose_temp',dw,if_exists='replace',index=False)