import pandas as pd
from sqlalchemy import create_engine

fq=create_engine('mysql+pymysql://dw_dev:hydw@2018@10.253.169.47/fqmall_ht_prod?charset=utf8')
fq=fq.connect()
data3=pd.read_excel('d:\\资金方全量表-0614.xlsx',header=0)
data3['亨元申请编号']=data3['亨元申请编号'].astype(str)

dakuan_sql="""SELECT
	a.app_No,
	b.BANK_NAME '打款银行',
	TOCASH_TYPE '提现方式',
	c.DIC_NAME '打款方式'
FROM
	cust_tocash a
LEFT JOIN hy_bank b on a.TC_BANK_CODE= b.BANK_CODE
LEFT JOIN d_dit_dic c on a.TOCASH_TYPE=c.dic_code"""


dakuan_data=pd.read_sql_query(dakuan_sql,fq)

dakuan_data['app_No']=dakuan_data['app_No'].astype(str)
data4=pd.merge(data3,dakuan_data,left_on='亨元申请编号',right_on='app_No',how='left').drop(['app_No'],axis=1)



data4.to_excel('d:\\资金方全量表-06142.xlsx',index=False)
print(data4.head())