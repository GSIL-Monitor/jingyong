import pandas as pd
from sqlalchemy import create_engine
import pymysql
import time
import datetime
from delorean import Delorean




t1=time.clock()
conn1=create_engine('mysql+pymysql://dw_dev:hydw@2018@10.253.169.47/ccms2?charset=utf8')
conn2=create_engine('mysql+pymysql://dw_dev:hydw@2018@10.253.169.47/dw_base?charset=utf8')



time1=(datetime.datetime.now()).strftime("%Y-%m-%d %H:%M:%S")

time2=(datetime.datetime.now()+datetime.timedelta(hours=6)).strftime("%Y-%m-%d %H:%M:%S")


datas= pd.read_sql('''select app_application_no,app_creditproduct_id,status,app_username,date(create_date) create_date,time(create_date) create_time,
                        app_site_code,app_cust_type,app_address,apply_amount,tranprice,app_install_ment,app_dis_count,app_month_pay,app_grade_work,provice,app_city_code
                        ,pay_type,incomeType,hy_industry_code from fqmall_ht_prod.d_application_pay''' ,conn2,chunksize=30000)

status= pd.read_sql('''select appstatus_key,status from dw_base.dim_appstatus''',conn2)
print(1)
# customer= pd.read_sql('''SELECT customer_key,user_name from dw_base.dim_customer''',conn2)
# print(2)
date= pd.read_sql('''SELECT date_key,date_value from dw_base.dim_date''',conn2)
print(3)
timesdata= pd.read_sql('''SELECT time_key,time_value from dw_base.dim_time''',conn2)
print(4)
t2=time.clock()
print(t2-t1)
dict1 = {}
yuan = ['app_application_no', 'app_creditproduct_id', 'appstatus_key', 'customer_key', 'date_key', 'time_key',
        'app_site_code', 'app_cust_type',
        'app_address', 'apply_amount', 'tranprice', 'app_install_ment', 'app_dis_count', 'app_month_pay',
        'app_grade_work', 'provice', 'app_city_code',
        'pay_type', 'incomeType', 'hy_industry_code'
        ]
new = ['app_number', 'app_creditproduct_key', 'app_status_key', 'app_user_key', 'app_create_date_key',
       'app_create_time_key', 'app_sale_key', 'app_cust_type_key', 'app_address', 'app_amount', 'app_tranPrice',
       'app_install_ment', 'app_dis_count', 'app_month_pay', 'app_grade_work_key', 'app_provice_key',
       'app_city_key', 'app_pay_type_key', 'app_inComeType', 'app_hy_industry_code']
for i in range(20):
    dict1[yuan[i]] = new[i]


for data in datas:
    data2=pd.merge(data,status,how='left',on='status').drop(['status'],axis=1)
    t2=time.clock()
    print(t2-t1)
    names = "','".join(['%s' for _ in range(len(data2['app_username']))])
    key_sql = """select user_name,customer_key from dw_base.dim_customer WHERE user_name in ('%s')""" % (names) % tuple(data2['app_username'].tolist())
    customer = pd.read_sql(key_sql, conn2)
    data3=pd.merge(data2,customer,left_on='app_username',right_on='user_name',how='left').drop(['app_username','user_name'],axis=1)
    t2=time.clock()
    print(t2-t1)

    data4=pd.merge(data3,date,left_on='create_date',right_on='date_value',how='left').drop(['create_date','date_value'],axis=1)
    t2=time.clock()
    print(t2-t1)

    data5=pd.merge(data4,timesdata,left_on='create_time',right_on='time_value',how='left').drop(['create_time','time_value'],axis=1)
    # print(data5.columns)


    t2=time.clock()


    data6=data5[yuan]
    data6.rename(columns=dict1,inplace=True)



    data6.to_sql('fact_order_application',con=conn2,index=False,if_exists='append')

    t2=time.clock()

    print(t2-t1)
