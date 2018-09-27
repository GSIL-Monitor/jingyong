import pandas as pd
from sqlalchemy import create_engine
import pymysql
import time
import numpy as np



# t1=time.clock()
# conn1=create_engine('mysql+pymysql://dw_dev:hydw@2018@10.253.169.47/ccms2?charset=utf8')
# conn2=create_engine('mysql+pymysql://dw_dev:hydw@2018@10.253.169.47/dw_base?charset=utf8')


# sql1= """select app_no,id_num,operator,collection_action,COLLECTION_TARGET,COLLECTION_RESULTS,COMMITMENT_AMOUNT,
# REPAYMENT_AMOUNT,LATE_STAGE,COLLECTION_TYPE,CASE_NOTES,CREATE_USER,UPDATE_USER,OUTSOURCING_COMPANY,
# PHONE_NUM,SOURCE_TYPE,DEFAULT_REASON,PAY_FLAG,DISPOSE_FLAG,FIRST_REASON,SEC_REASON,
# UPLOAD_FLAG,ACTION_NUM,COLLECTION_DATE,COLLECTION_DATE,date(CREATE_TIME) create_date,time(CREATE_TIME) create_min,
# date(UPDATE_TIME) update_date,time(UPDATE_TIME) update_min from pu_debt_recoard"""


data1=pd.DataFrame({"A":[1,2,3,4],"B":[2,3,4,5]})

data2=pd.DataFrame({"A":[12,13,1],"B":[10,11,12]})
print(len(data1["A"]))
print(np.intersect1d(data1['A'],data2['A'])[0])