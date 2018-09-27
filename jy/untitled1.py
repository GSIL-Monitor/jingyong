# -*- coding: utf-8 -*-
"""
Created on Wed May  2 11:01:04 2018

@author: 123
"""

import pandas as pd
from sqlalchemy import create_engine
import pymysql
conn1=create_engine('mysql+pymysql://dw_dev:hydw@2018@10.253.169.47/ccms2?charset=utf8')
conn2=create_engine('mysql+pymysql://dw_dev:hydw@2018@10.253.169.47/dw_base?charset=utf8')
conn3=create_engine('mysql+pymysql://dw_dev:hydw@2018@10.253.169.47/fqmall_ht_prod?charset=utf8')

sql= """select app_application_no,app_creditproduct_id,status,app_username,date(create_date) credate_date,time(create_date) cteatr_time,
app_site_code,app_cust_type,app_address,apply_amount,tranprice,app_install_ment,app_dis_count,app_month_pay,app_grade_work,provice,app_city_code
,pay_type,incomeType,hy_industry_code from d_application_pay
"""

data= pd.read_sql(sql,conn3)


status_sql='select appstatus_key,status from dim_appstatus'

status= pd.read_sql(status_sql,conn2)


data=pd.merge(data,status,left_on='status',right_on='status',how='left')