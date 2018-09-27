import pandas as pd
from sqlalchemy import create_engine
import datetime
import time
from conn import *
import numpy as np
import calendar
fq=fq()
dw=dw()

#----应还——


loan_loan_sql = """
SELECT id,APPLICATION_ID FROM loan_loan  WHERE CREATE_TIME > '2018-03-01';
"""
fact_order_verify_sql = """
    SELECT ver_number,ver_user_type from fact_order_verify  WHERE ver_create_date >= '2018-03-01';    
"""
loan_repay_plan_sql="""
    SELECT due_date,loan_id,PRINCIPAL,INTEREST,MTH_FEE,LPC,PENALTY_INT,TOTAL_AMT,info_fee,add_fee1,add_fee2,add_fee3,add_fee4 FROM loan_repay_plan a WHERE a.DUE_DATE >'2018-03-01';
"""
loan_loan_date = pd.read_sql(loan_loan_sql,fq)
fact_order_verify_date = pd.read_sql(fact_order_verify_sql,dw)
loan_repay_plan_data=pd.read_sql(loan_repay_plan_sql,fq)



today='2018-06-01'#datetime.datetime.today().date()
year=datetime.datetime.today().year
month=datetime.datetime.today().month
last_day=datetime.datetime.today().date()-datetime.timedelta(days=1)

day_list=[(x+datetime.timedelta(days=1)).strftime('%Y-%m-%d') for x in pd.date_range(end='%s'%today,periods=6,freq='M')]

today,one,two,three,four,five=sorted(day_list,reverse=True)
loan_repay_plan_sql="""
    SELECT due_date,loan_id,PRINCIPAL,INTEREST,MTH_FEE,LPC,PENALTY_INT,TOTAL_AMT,info_fee,add_fee1,add_fee2,add_fee3,add_fee4 FROM loan_repay_plan a WHERE a.DUE_DATE >'2018-03-01';
"""
loan_repay_plan_data=pd.read_sql(loan_repay_plan_sql,fq)
temp_date = pd.merge(loan_loan_date,fact_order_verify_date,left_on='APPLICATION_ID',right_on='ver_number',how='right')
final_date = pd.merge(temp_date,loan_repay_plan_data,left_on='id',right_on='loan_id',how='left')

final_date.drop(['id','APPLICATION_ID','ver_number','loan_id'],axis=1,inplace=True)
final_date['due_date']=final_date['due_date'].astype('str')
final_date2=final_date[final_date['due_date']<today]
final_date3=final_date2.groupby('ver_user_type').agg(np.sum)
final_date4=final_date3.reset_index()


print('yinghuan___done')


#---------已还

def yihuan():
    acct_tran_detial_sql = """
        SELECT
        loan_id,
        CASE
    WHEN tran_detail_type = 'BXFA' THEN
        'BXFD'
    WHEN tran_detail_type = 'BXFB' THEN
        'BXFD'
    WHEN tran_detail_type = 'RPEP' THEN
        'RPLD'
    WHEN tran_detail_type = 'RPEI' THEN
        'RPLI'
    ELSE
        tran_detail_type
    END,
     abs(amount),
     date(create_time)
    FROM
        acct_transaction_detail
    WHERE
        CREATE_TIME > '2018-03-01' and TRAN_DETAIL_TYPE in('RPLD','RPLI','RPEP','RPEI','RPPI','RPLP','RPMF','XXFU','BXFD','BXFA','BXFB')
    """
    acct_tran_detial_date = pd.read_sql(acct_tran_detial_sql,fq)

    temp_date = pd.merge(loan_loan_date,fact_order_verify_date,left_on='APPLICATION_ID',right_on='ver_number',how='right')

    final_date = pd.merge(temp_date,acct_tran_detial_date,left_on='id',right_on='loan_id',how='left')

    final_date.drop(['id','APPLICATION_ID','ver_number','loan_id'],axis=1,inplace=True)
    final_date.columns=['ver_user_type','tran_detail_type','amount','create_time']
    final_date['create_time']=final_date['create_time'].astype('str')
    fin_two=final_date[final_date['create_time']<today]#datetime.datetime.strptime(str(final_date['create_time'])<datetime.datetime.strptime(two,'%Y-%m-%d')
    fin_two=fin_two.pivot_table(index=['ver_user_type','tran_detail_type'],values='amount',aggfunc=np.sum).reset_index()
    fin_two2=fin_two.pivot_table(index='ver_user_type',columns='tran_detail_type',values='amount').reset_index(0)
    print('yihuan___done')

    return fin_two2


if __name__=='__main__':
    yihuan_data=yihuan()
    fin_all= pd.merge(yihuan_data,final_date4,on='ver_user_type',how='left')
    fin_all['month']=last_day
    fin_all.to_sql('ceo_month_amount_report',dw,index=False,if_exists='append')
