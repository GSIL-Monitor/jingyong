import pandas as pd
from sqlalchemy import create_engine
import pymysql
import time
import datetime
from update_current_load_time import update_current_time
update_current_time('fact_loan')



fq=create_engine('mysql+pymysql://dw_dev:hydw@2018@10.253.169.47/fqmall_ht_prod?charset=utf8')
fq=fq.connect()
dw=create_engine('mysql+pymysql://dw_dev:hydw@2018@10.253.169.47/dw_base?charset=utf8')
dw=dw.connect()
datedata = pd.read_sql('''SELECT date_key,date_value from dw_base.dim_date''', dw)

timesdata = pd.read_sql('''SELECT time_key,time_value from dw_base.dim_time''', dw)

productdata= pd.read_sql('''SELECT a.ID,a.investor_name,b.finance_key from fqmall_ht_prod.loan_product a
LEFT JOIN dw_base.dim_finance b
on a.investor_name=b.finance_code''',dw)
staff_data= pd.read_sql('''select staff_key as loan_create_staff_key,user_name from dim_staff''',dw)

cash_sql="""SELECT
	a.ID as loan_id,
	a.USERNAME,
	DATE(a.CREATE_TIME) create_date,
	TIME(a.CREATE_TIME) create_time,
	DATE(a.END_DATE) end_date,
	a.create_user,
	a.PRODUCT_ID,
	a.PURPOSE as loan_purpose,a.APPLICATION_ID as loan_hy_id,b.PRINCIPAL as loan_principal,	b.TOTAL_TERM as loan_total_term,b.LOAN_INT_RATE as loan_interest_rate,b.REPAY_TYPE as loan_repay_type,
	DATE(b.AVAILABLE_DATE) as available_date,
	TIME(b.AVAILABLE_DATE) as available_time,
	b.TERMINATION_IND as loan_termination_ind,
	DATE(b.TERMINATION_DATE) as  termination_date,
	b.REMAIN_PRIN as loan_remain_prin,b.REMAIN_INT as loan_remain_int,b.REMAIN_FEE as loan_remian_fee,b.unpaid_info_fee as loan_unpaid_info_fee,b.unpaid_add_fee1 as loan_unpaid_add_fee1,
	b.unpaid_add_fee2 as loan_unpaid_add_fee2,b.unpaid_add_fee3 as loan_unpaid_add_fee3,b.unpaid_add_fee4 as loan_unpaid_add_fee4,b.CURRENT_OT_FEE as loan_current_of_fee,b.CURRENT_LPC as loan_current_lpc,
	a.amount as loan_amount
from loan_loan as a
LEFT JOIN loan_loan_acc as b
on a.id =b.LOAN_ID

"""

cash_datas=pd.read_sql(cash_sql,fq,chunksize=30000)
start = (datetime.datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
for cash_data in cash_datas:
    ids = "','".join(['%s' for _ in range(len(cash_data['USERNAME']))])
    user_sql = """select customer_key as loan_user_key,user_name,max(DISTINCT load_start) load_start from dw_base.dim_customer_copy WHERE user_name in ('%s') """ % (ids) % tuple(cash_data['USERNAME'].tolist())
    user_data = pd.read_sql(user_sql,dw)

    data1=pd.merge(cash_data,datedata,left_on='create_date',right_on='date_value',how='left').drop(['create_date','date_value'],axis=1)
    data2=pd.merge(data1,timesdata,left_on='create_time',right_on='time_value',how='left').drop(['create_time','time_value'],axis=1)
    data3=pd.merge(data2,datedata,left_on='available_date',right_on='date_value',how='left').drop(['available_date','date_value'],axis=1)
    data4=pd.merge(data3,timesdata,left_on='available_time',right_on='time_value',how='left').drop(['available_time','time_value'],axis=1)
    data5=pd.merge(data4,datedata,left_on='end_date',right_on='date_value',how='left').drop(['end_date','date_value'],axis=1)
    print(data5.columns)
    data5.columns=['loan_id', 'USERNAME', 'create_user', 'PRODUCT_ID', 'loan_purpose',
           'loan_hy_id', 'loan_principal', 'loan_total_term', 'loan_interest_rate',
           'loan_repay_type', 'loan_termination_ind', 'termination_date',
           'loan_remain_prin', 'loan_remain_int', 'loan_remian_fee',
           'loan_unpaid_info_fee', 'loan_unpaid_add_fee1', 'loan_unpaid_add_fee2',
           'loan_unpaid_add_fee3', 'loan_unpaid_add_fee4', 'loan_current_of_fee',
           'loan_current_lpc', 'loan_amount', 'loan_create_date_key', 'loan_create_time_key',
           'loan_available_date_key', 'loan_available_time_key', 'loan_end_date_key']
    data6=pd.merge(data5,datedata,left_on='termination_date',right_on='date_value',how='left').drop(['termination_date','date_value'],axis=1)
    data7=pd.merge(data6,user_data,left_on='USERNAME',right_on='user_name',how='left').drop(['USERNAME','user_name','load_start'],axis=1)
    data8=pd.merge(data7,productdata,left_on='loan_id',right_on='ID',how='left').drop(['ID','investor_name','PRODUCT_ID'],axis=1)
    print(data8.columns)
    data8.columns=['loan_id', 'create_user', 'loan_purpose', 'loan_hy_id',
           'loan_principal', 'loan_total_term', 'loan_interest_rate',
           'loan_repay_type', 'loan_termination_ind', 'loan_remain_prin',
           'loan_remain_int', 'loan_remian_fee', 'loan_unpaid_info_fee',
           'loan_unpaid_add_fee1', 'loan_unpaid_add_fee2', 'loan_unpaid_add_fee3',
           'loan_unpaid_add_fee4', 'loan_current_of_fee', 'loan_current_lpc',
           'loan_amount', 'loan_create_date_key', 'loan_create_time_key',
           'loan_available_date_key', 'loan_available_time_key',
           'loan_end_date_key', 'loan_termination_date_key', 'loan_user_key', 'loan_investor_key']

    data9=pd.merge(data8,staff_data,left_on='create_user',right_on='user_name',how='left').drop(['create_user','user_name'],axis=1)
    try:
        data9.to_sql('fact_loan',dw,index=False,if_exists='append')
    except pymysql.err.IntegrityError as e :
        print(e)
    print('done')



