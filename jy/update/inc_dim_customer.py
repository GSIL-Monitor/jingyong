import pandas as pd
from sqlalchemy import create_engine
import pymysql
import time
import datetime
from sqlalchemy.orm import sessionmaker
import sys
sys.path.append('/home/cl/scrip')
from update_current_load_time import update_current_time
from update_last_load_time import update_last_time
from conn import *

last_time,current_time=update_current_time('dim_customer')



t1 = time.clock()
conn1=fq()
conn2=dw()
conn3 = pymysql.connect(
    host="10.253.169.47",
    user="otter",
    passwd="otter@123",
    charset="utf8",
    db='dw_base',
    use_unicode=False
)
cur3 = conn3.cursor()


t1 = time.clock()
start = (datetime.datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
day = (datetime.datetime.now()).strftime("%Y-%m-%d")
time1 = (datetime.datetime.now()).strftime("%H:%M:%S")
time2 = (datetime.datetime.now() - datetime.timedelta(minutes=10)).strftime("%H:%M:%S")

user = """SELECT id,username as user_name,email_address,credit_level,create_time ,MOBILENO as mobile,
is_old_user ,register_from,cust_from,double_sales,app_flag
FROM fqmall_ht_prod.CUST_USER where username is not NULL 
and create_time > \'%s\' 
and create_time < \'%s\' 
""" % (last_time, current_time)
print(user)
data_user = pd.read_sql(user, conn1)
print('共查处：%d条数据' % len(data_user.index))

if data_user.empty:
    print('此时间段内无数据')
else:
    names = "','".join(['%s' for _ in range(len(data_user['user_name']))])

    customer = """SELECT USERNAME as user_name,
    identity_no,gender,cancel_flag,cancel_desc,
    NAME as real_name,
    unitName as work_unit,
    customerType as customer_type,
    workExperience as work_experience,
    emergency_mobile,emergency_name,emergency_Relation,immediate_mobile,immediate_Name,immediate_Relation,now_education,spouse_mobile,spouse_name,permanentAddress_area,
    permanentAddress_city,permanentAddress_province,permanentAddress_raod,
    work_Area_Area as work_unit_area,
    work_Area_City as work_Area_city,
    work_Area_Province as work_Area_province,
    work_Area_Road as work_Area_road,
    housing_Conditions,QQNumber,
    NATION,IDCARD_VALIDITY_STARTDATE,IDCARD_VALIDITY_ENDDATE,fulltimeDriver as fulltime_driver ,'1700-01-01 00:00:00' as load_start,'2999-01-01 00:00:00'as load_end 
    FROM fqmall_ht_prod.`cust_customer` where USERNAME in ('%s')""" % (names) % tuple(
        data_user['user_name'].tolist())

    print(customer)
    # key_sql = """select user_name,customer_key from dw_base.dim_customer WHERE USERNAME in ('%s')""" % (names) % tuple(data['user_name'].tolist())
    data_customer = pd.read_sql(customer, conn1, parse_dates=False)


    print('开始-----连接')
    data_all = pd.merge(data_user, data_customer, on='user_name', how='outer')
    # print(data_all.columns)


    up_sql = """update dw_base.dim_customer set load_end='%s' WHERE user_name in ('%s')""" % (start,names) % tuple(data_user['user_name'].tolist())
    cur3.execute(up_sql)
    conn3.commit()
    print('更新完成')

    new_sql = """select user_name from dw_base.dim_customer WHERE user_name in ('%s')""" % (names) % tuple(
        data_user['user_name'].tolist())
    data_new = pd.read_sql(new_sql, conn1)
    print(new_sql)
    print('重复数据:%d条'%len(data_new.index))
    if data_new.empty:
        data_all.to_sql('dim_customer', conn2, index=False, if_exists='append')
    else:
        data_all.loc[data_all['user_name'].isin(list(data_new['user_name'])),'load_start']=start
        data_all.to_sql('dim_customer', conn2, index=False, if_exists='append')
        print('新增数据更新完成')


    t2 = time.clock()
    print(t2 - t1)
conn_close()
update_last_time('dim_customer')