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
loan_sql="""select create_time,product_id,application_id,username,amount from loan_loan"""
loan=pd.read_sql(loan_sql,fq)
product_sql="""select id,name from loan_product"""
product=pd.read_sql(product_sql,fq)

# data=pd.read_excel('d:\\失信\\0515.xlsx',header=None,names=['name','iden'])
# data.drop_duplicates(inplace=True)
# data1=pd.merge(data,cus,left_on=['name','iden'],right_on=['name','mobile'],how='left')
# data2=pd.merge(data1,cus,left_on=['name','iden'],right_on=['name','identity_no'],how='left')
# nulls=data2['mobile_x'].isnull()
# data2.loc[nulls,'mobile_x':'username_x']=data2.loc[nulls,'mobile_y':'username_y']
# # loan_acc_sql="""select principal,loan_id from loan_loan_acc"""
# data3=data2.drop(['mobile_y','identity_no_y','username_y'],axis=1)
# data4=pd.merge(data3,loan,left_on='username_x',right_on='username',how='left')
# data5=pd.merge(data4,product,left_on='product_id',right_on='id').drop(['iden','username_x','product_id','id','username'],axis=1)
# data5.to_excel('d:\\05152.xlsx',index=False)
# print(data2)


#
# def step():
#     data = pd.read_excel('d:\\失信\\0428.xlsx', header=None, names=['name'])
#     data.drop_duplicates(inplace=True)
#     data1 = pd.merge(data, cus, on='name', how='left')
#     data2 = pd.merge(data1, cus, left_on='name', right_on='identity_no', how='left')
#     data3 = pd.merge(data2, cus, left_on='name_x', right_on='mobile', how='left',suffixes=('_z'))
#     null2=data3['mobile_x'].isnull()
#     data3.loc[null2, 'mobile_x':'username_x'] = data3.loc[null2, 'mobile_y':'username_y']
#     # null2=data3['mobile_x'].isnull()
#     data3.loc[null2, 'mobile_x':'username_x'] = data2.loc[null2, 'mobile_z':'username_z']
#     data4=pd.merge(data3,loan,left_on='username_x',right_on='username',how='left')
#     data5 = pd.merge(data4, product, left_on='product_id', right_on='id')
#     data5.drop_duplicates(inplace=True)
#     data5.to_excel('d:\\0428.xlsx', index=False)

# step()


def step2():

    data=pd.read_sql_query("""select name,if(mobile='',id_card_no,mobile) iden from lose_credit""",dw,coerce_float=False)
    # data.drop_duplicates(inplace=True)
    data1=pd.merge(data,cus,left_on=['name','iden'],right_on=['name','mobile'],how='left')
    data2=pd.merge(data1,cus,left_on=['name','iden'],right_on=['name','identity_no'],how='left')
    nulls=data2['mobile_x'].isnull()
    data2.loc[nulls,'mobile_x':'username_x']=data2.loc[nulls,'mobile_y':'username_y']
    # loan_acc_sql="""select principal,loan_id from loan_loan_acc"""
    # data3=data2.drop(['mobile_y','identity_no_y','username_y'],axis=1)
    data4=pd.merge(data2,loan,left_on='username_x',right_on='username',how='left')
    data5=pd.merge(data4,product,left_on='product_id',right_on='id')#.drop(['iden','username_x','product_id','id','username'],axis=1)
    data5.to_excel('d:\\lose_credit.xlsx',index=False)
    print(data2)

step2()
# sql2="""SELECT a.name,a.mobile,a.identity_no,b.CREATE_TIME,b.product_id,b.application_id,c.principal
# from cust_customer a
# LEFT JOIN loan_loan b on a.username=b.username
# LEFT JOIN loan_loan_acc c on b.id=c.loan_id"""
# data2=pd.read_sql(sql2,fq)

# data3=pd.merge(data,data2,left_on='iden',right_on=['mobile','identity_no'],how='left')
print(111)