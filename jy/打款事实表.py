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
citydata = pd.read_sql('''select city_key as cash_city_key,city_code from dim_city''',dw)
provincedata = pd.read_sql('''select province_key as cash_province_key,province_code from dim_province''',dw)

to_cash_sql="""SELECT
	ID as cash_id,	TC_AMOUNT as cash_amount,	TC_APPLICANT as cash_applicant,	TC_CARD_NAME as cash_card_name,	TC_CARD_NUM as cash_card_num,	TC_FLOW_NO as cash_flow_no,
	DATE(TC_START_TIME) as cash_start_date,	TIME(TC_START_TIME) as cash_start_time,	TC_STATUS as cash_status,	TC_BANK_CODE as cash_back_code,	USERNAME as cash_username,TC_FEE as cash_fee,
	PROVINCE_VAR ,	
	CITY_VAR,
	BANK_VAR as cash_bank_var,	IS_CREATE_PAY_FILE as cash_is_create_pay,	PAY_BATCH_NO as cash_pay_batch_no,
	DATE(CREATE_TIME) as create_date,
	TIME(CREATE_TIME) as create_time,	
	SUPPLIER_ID as cash_supplier_id,SUPPLIER_NAME as cash_supplier_name,SUPPLIER_TEL as cash_supplier_tel,	REAL_NAME as cash_real_name,tc_type as cash_type,app_No as cash_app_no,	operator_User as cash_operator_user,
	DATE(CONFIRM_DATE) as confirm_date,
	TIME(CONFIRM_DATE) as confirm_time,
	CONFIRM_USER as cash_confrim_user,	TOCASH_TYPE as cash_tocash_type,
	PAY_AMOUNT_FLAG as cash_pay_amount_flag,SERIAL_NO as cash_serial_no,BANKVAR_NO as cash_bankvar_no,	sms_flag as cash_sms_flag,	acc_num as cash_acc_num,
	recharge_type as cash_recharge_type,hy_industry_code as cash_hy_industry_code,	is_accounts as cash_is_accounts,batch_flag as cash_batch_flag,bank_net as cash_bank_net,
	PAY_MARK as cash_pay_mark
FROM
	fqmall_ht_prod.cust_tocash

"""
cash_data= pd.read_sql(to_cash_sql,fq)
print(1)
data1=pd.merge(cash_data,citydata,left_on='CITY_VAR',right_on='city_code',how='left').drop(['CITY_VAR','city_code'],axis=1)
print(2)

data2= pd.merge(data1,provincedata,left_on='PROVINCE_VAR',right_on='province_code',how='left').drop(['PROVINCE_VAR','province_code'],axis=1)
print(3)

data3= pd.merge(data2,datedata,left_on='create_date',right_on='date_value',how='left').drop(['create_date','date_value'],axis=1)
print(4)

data4= pd.merge(data3,datedata,left_on='confirm_date',right_on='date_value',how='left').drop(['confirm_date','date_value'],axis=1)
print(5)

data5= pd.merge(data4,timesdata,left_on='create_time',right_on='time_value',how='left').drop(['create_time','time_value'],axis=1)
print(6)

data6= pd.merge(data5,timesdata,left_on='confirm_time',right_on='time_value',how='left').drop(['confirm_time','time_value'],axis=1)
print(7)
data7= pd.merge(data6,datedata,left_on='cash_start_date',right_on='date_value',how='left').drop(['cash_start_date','date_value'],axis=1)
print(8)

data8= pd.merge(data7,timesdata,left_on='cash_start_time',right_on='time_value',how='left').drop(['cash_start_time','time_value'],axis=1)
print(9)


data8.columns=['cash_id', 'cash_amount', 'cash_applicant', 'cash_card_name',
       'cash_card_num', 'cash_flow_no', 'cash_status', 'cash_back_code', 'cash_username', 'cash_fee',
       'cash_bank_var', 'cash_is_create_pay', 'cash_pay_batch_no',
       'cash_supplier_id', 'cash_supplier_name', 'cash_supplier_tel',
       'cash_real_name', 'cash_type', 'cash_app_no', 'cash_operator_user',
       'cash_confrim_user', 'cash_tocash_type', 'cash_pay_amount_flag',
       'cash_serial_no', 'cash_bankvar_no', 'cash_sms_flag', 'cash_acc_num',
       'cash_recharge_type', 'cash_hy_industry_code', 'cash_is_accounts',
       'cash_batch_flag', 'cash_bank_net', 'cash_pay_mark', 'cash_city_key',
       'cash_province_key', 'cash_create_date_key', 'cash_confrim_date_key', 'cash_create_time_key',
       'cash_confrim_time_key','cash_start_date', 'cash_start_time']



# data6['cash_start_time']=data6['cash_start_time'].apply(lambda x: ''.join(x))

# data6['cash_start_time']=pd.to_datetime(data6['cash_start_time'],format='%H:%M:%S')


# data6['cash_start_time']=pd.to_timedelta(data6['cash_start_time'],unit='s')

data8.to_sql('fact_tocash',dw,index=False,if_exists='append',chunksize=30000)

fq.close()
dw.close()
print(data6)




