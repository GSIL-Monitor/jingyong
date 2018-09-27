import pandas as pd
from sqlalchemy import create_engine

cc=create_engine('mysql+pymysql://dw_dev:hydw@2018@10.253.169.47/ccms2?charset=utf8')
cc=cc.connect()
fq=create_engine('mysql+pymysql://dw_dev:hydw@2018@10.253.169.47/fqmall_ht_prod?charset=utf8')
fq=fq.connect()
dw=create_engine('mysql+pymysql://dw_dev:hydw@2018@10.253.169.47/dw_base?charset=utf8')
dw=dw.connect()

# data=pd.read_excel('d:\\333.xlsx',header=0,sheet_name='ss')
# data.to_sql('sss_temp',dw,index=False)
sql="""SELECT f.*,b.due_date FROM
(SELECT * from dw_base.sss_temp) f
LEFT JOIN
(SELECT id,APPLICATION_ID	FROM fqmall_ht_prod.loan_loan) a
on f.application_no=a.application_id
LEFT JOIN
(SELECT loan_id,DUE_DATE from fqmall_ht_prod.loan_repay_plan WHERE CURRENT_TERM=1)b
on a.id=b.loan_id
"""
data2=pd.read_sql(sql,fq)
# data2['APPLICATION_ID']=data2['APPLICATION_ID'].astype(int)
# data3=pd.merge(data,data2,left_on='application_no',right_on='APPLICATION_ID',how='left')
data2.to_excel('d:\\第一期日期查询.xlsx')
print(data2)