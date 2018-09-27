import pandas as pd
from sqlalchemy import create_engine
import datetime
import time
import pymysql
# real=datetime.datetime.now().date()
# today=datetime.datetime.now().date()-datetime.timedelta(days=1)
# conn3 = pymysql.connect(
#     host="10.253.169.47",
#     user="dw_dev",
#     passwd="hydw@2018",
#     charset="utf8",
#     db='dw_base',
#     use_unicode=False
# )
# cur3=conn3.cursor()

cc=create_engine('mysql+pymysql://dw_dev:hydw@2018@10.253.169.47/ccms2?charset=utf8')
cc=cc.connect()
fq=create_engine('mysql+pymysql://dw_dev:hydw@2018@10.253.169.47/fqmall_ht_prod?charset=utf8')
fq=fq.connect()
dw=create_engine('mysql+pymysql://dw_dev:hydw@2018@10.253.169.47/dw_base?charset=utf8')
dw=dw.connect()
test=create_engine('mysql+pymysql://coretest:Hengyuan@0326@10.253.3.69/test?charset=utf8')
test=test.connect()


cus_sql="""select name,mobile,identity_no,username from cust_customer"""
cus=pd.read_sql(cus_sql,fq)
# loan_sql="""select create_time,product_id,application_id,username,amount from loan_loan"""
# loan=pd.read_sql(loan_sql,fq)
# product_sql="""select id,name from loan_product"""
# product=pd.read_sql(product_sql,fq)



#
def step():
    data = pd.read_excel('d:\\失信\\0428.xlsx', header=None, names=['name'])
    data.drop_duplicates(inplace=True)
    data['tag']=1
    print(data)
    data1 = pd.merge(data, cus, on='name', how='left')
    data2 = pd.merge(data1, cus, left_on='name', right_on='identity_no', how='left')
    data3 = pd.merge(data2, cus, left_on='name_x', right_on='mobile', how='left')
    null2=data3['mobile_x'].isnull()
    data3.loc[null2, 'mobile_x':'username_x'] = data3.loc[null2, 'mobile_y':'username_y']
    data3.columns=['name','mobile','identity_no','username','name1','mobile1','identity_no1','username1','name2','mobile2','identity_no2','username2']
    null3=data3['mobile'].isnull()
    data3.iloc[null3, 'mobile':'username'] = data3.loc[null3, 'mobile2':'username2']
    # data4=pd.merge(data3,loan,left_on='username',right_on='username',how='left')
    # data5 = pd.merge(data4, product, left_on='product_id', right_on='id')
    # data5.drop_duplicates(inplace=True)
    # data5.to_excel('d:\\0428.xlsx', index=False)

step()



print(111)