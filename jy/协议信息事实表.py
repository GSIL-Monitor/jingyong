import pandas as pd
from sqlalchemy import create_engine
import pymysql
import time
import datetime
from update_current_load_time import update_current_time
update_current_time('fact_loan')



conn1=create_engine('mysql+pymysql://dw_dev:hydw@2018@10.253.169.47/fqmall_ht_prod?charset=utf8')
conn1=conn1.connect()
conn2=create_engine('mysql+pymysql://dw_dev:hydw@2018@10.253.169.47/dw_base?charset=utf8')
conn2=conn2.connect()
datedata = pd.read_sql('''SELECT date_key,date_value from dw_base.dim_date''', conn2)

timesdata = pd.read_sql('''SELECT time_key,time_value from dw_base.dim_time''', conn2)

down_sql="""SELECT
	id AS prot_id,
	DATE(create_date) as create_date,
	TIME(create_date) as create_time,
	create_user as prot_create_user,
	DATE(update_date) as update_date,
	TIME(update_date) as update_time,
	update_user as prot_update_user	, 
	application_no as prot_application_no,
	LEFT (application_no,14) AS loan_id,
	origin as prot_origin,
	save_path as prot_save_path,
	cfcaFlag as prot_cfca_flag,
	protocol_type as prot_type
FROM
	fqmall_ht_prod.d_protocol_download
"""

down_datas= pd.read_sql(down_sql,conn1,chunksize=30000)
print(2)
# for down_data in down_datas:
# print(len(down_datas['loan_id']))
for down_data in down_datas:
    # down_data['loan_id'] = pd.to_numeric(down_data['loan_id'],downcast='signed')
    data1=pd.merge(down_data,datedata,left_on='create_date',right_on='date_value',how='left').drop(['date_value','create_date'],axis=1)
    print(1)
    data2=pd.merge(data1,timesdata,left_on='create_time',right_on='time_value',how='left').drop(['time_value','create_time'],axis=1)
    print(3)
    data3=pd.merge(data2,datedata,left_on='update_date',right_on='date_value',how='left').drop(['date_value','update_date'],axis=1)
    print(4)
    data4=pd.merge(data3,timesdata,left_on='update_time',right_on='time_value',how='left').drop(['time_value','update_time'],axis=1)
    print(5)
    data4.columns=[
        'prot_id','prot_create_user','prot_update_user',
        'prot_application_no','loan_id','prot_origin','prot_save_path',
        'prot_cfca_flag','prot_type','prot_create_date_key',
        'prot_create_time_key','prot_update_date_key','prot_update_time_key'

    ]
    print(len(data4['loan_id']))

    ids = "','".join(['%s' for _ in range(len(data4['loan_id']))])
    amount_sql = """select DISTINCT application_id,amount as prot_amount  from fqmall_ht_prod.loan_loan WHERE application_id in ('%s')""" % (ids) % tuple(data4['loan_id'].tolist())
    amount_data= pd.read_sql(amount_sql,conn1)
    print(6)
    amount_data['application_id']=amount_data['application_id'].astype(str)



    data5=pd.merge(data4,amount_data,left_on='loan_id',right_on='application_id',how='left').drop(['loan_id','application_id'],axis=1)
    print(7)



    data5.to_sql('fact_protocol_info',conn2,index=False,if_exists='append')
    print(data5.head())
conn1.close()
conn2.close()