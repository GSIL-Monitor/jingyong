import pandas as pd
from sqlalchemy import create_engine
import pymysql
import time
import datetime



t1=time.clock()
conn1=create_engine('mysql+pymysql://dw_dev:hydw@2018@10.253.169.47/fqmall_ht_prod?charset=utf8')
conn2=create_engine('mysql+pymysql://dw_dev:hydw@2018@10.253.169.47/dw_base?charset=utf8')
conn3 = pymysql.connect(
    host="10.253.169.47",
    user="dw_dev",
    passwd="hydw@2018",
    charset="utf8",
    db='dw_base',
    use_unicode=False
)
cur3=conn3.cursor()

sql1="""dnfkjsdnfjnsd"""
cur3.execute(sql1)
conn3.commit()
conn3.close()



user= """SELECT id,username as user_name,email_address,credit_level,DATE(create_time) create_date,TIME(creaTE_TIME) create_time,MOBILENO as mobile,
is_old_user ,register_from,cust_from,double_sales,app_flag
FROM fqmall_ht_prod.CUST_USER where username is not NULL 
"""

customer="""SELECT USERNAME as user_name,
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
        FROM fqmall_ht_prod.`cust_customer` where USERNAME is not NULL """

data_user=pd.read_sql(user,conn1,parse_dates=False)
data_customer= pd.read_sql(customer,conn1,parse_dates=False)

data_all= pd.merge(data_user,data_customer,on='user_name',how='outer')
print(data_all.columns)

data_all.to_sql('dim_customer_copy',conn2,index=False)
t2=time.clock()
print(t2-t1)

