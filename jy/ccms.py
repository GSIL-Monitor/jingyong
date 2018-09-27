import pandas as pd
from conn import *


# cc_data,是直接从催记表里取出来要的字段，存入的。因为当时的那个库要维护，所以存入我们的大数据库的
data=pd.read_sql('select * from cc_data',dw())
dic=pd.read_sql("""SELECT DIC_CODE,DIC_NAME from pu_dit_dic""",cc())
data2=pd.merge(data,dic,left_on='action_num',right_on='DIC_CODE',how='left')
data2.drop_duplicates(inplace=True)

#  找出每个订单具体的产品类型、身份证号
sql1="""
SELECT a.APPLICATION_ID,a.AMOUNT,DATE(a.CREATE_TIME),c.name,b.TOTAL_TERM,b.LOAN_ID FROM loan_loan a
LEFT JOIN loan_loan_acc b
ON a.ID=b.`LOAN_ID`
LEFT JOIN loan_product c
on a.PRODUCT_ID=c.ID"""
user=pd.read_sql(sql1,fq())
d_sql="""select app_application_no,app_user_identityno from d_application_pay"""
d_data=pd.read_sql(d_sql,fq())
uss=pd.merge(user,d_data,left_on='APPLICATION_ID',right_on='app_application_no',how='left')
uss.drop(['app_application_no'],axis=1,inplace=True)

#通过订单号进行匹配
combin=pd.merge(data2,uss,left_on='app_no',right_on='APPLICATION_ID',how='left')


#通过身份证号进行匹配
combin2=pd.merge(combin,uss,left_on='id_num',right_on='app_user_identityno',how='left')


# combin2.drop_duplicates(inplace=True)



#对join后的数据进行清洗
def func(data3s,a):
    nulls=data3s[a+'_x'].isnull()
    data3s.loc[nulls,a+'_x']=data3s.loc[nulls,a+'_y']
    data3s.drop([a+'_y'],axis=1,inplace=True)

for i in ['APPLICATION_ID','AMOUNT','DATE(a.CREATE_TIME)','name','TOTAL_TERM','LOAN_ID','app_user_identityno']:
    func(combin2,i)

combin2.drop(['app_no','DIC_CODE'],axis=1,inplace=True)
print(combin2.columns)
combin2.columns=['id_num','collection_date','case_notes','collection_results','late_stage','action_num','dic_name','application_id','amount','create_time','name','total_term','loan_id','identity']
combin2['name']=combin2['name'].astype('str')

product_type=[]
for i in combin2.name.tolist():
    if i.startswith('VIP') or i.startswith('嗨秒贷'):
        product_type.append('new')
    else:
        product_type.append('old')

combin2['product_type']=product_type
combin2.drop(['name'],axis=1)

combin2.drop_duplicates(inplace=True)
combin2.to_csv('/home/testuser/finall.txt',index=False)