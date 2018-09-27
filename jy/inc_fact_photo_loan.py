import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime,timedelta
import time
fq=create_engine('mysql+pymysql://dw_dev:hydw@2018@10.253.169.47/fqmall_ht_prod?charset=utf8')
fq=fq.connect()
dw=create_engine('mysql+pymysql://dw_dev:hydw@2018@10.253.169.47/dw_base?charset=utf8')
dw=dw.connect()
cc=create_engine('mysql+pymysql://dw_dev:hydw@2018@10.253.169.47/ccms2?charset=utf8')
cc=cc.connect()

today=datetime.today()


loan_sql="""SELECT
	ll.ID loan_id,
	ll.APPLICATION_ID application_no,
	llc.TERMINATION_IND is_end,
	llc.CUR_TERM current_term
FROM
	fqmall_ht_prod.loan_loan_acc llc
LEFT JOIN fqmall_ht_prod.loan_loan ll ON ll.ID = llc.LOAN_ID
where llc.create_time BETWEEN DATE_SUB(date(NOW()),INTERVAL 1 DAY) and DATE(now())
"""
loan_data=pd.read_sql(loan_sql,fq)

ids = "','".join(['%s' for _ in range(len(loan_data['loan_id']))])
loanids='%s'% (ids) % tuple(loan_data['loan_id'].tolist())
print(loanids)
loan_repay_sql="""SELECT
	DUE_DATE,
	LOAN_ID loan_id,
	sum(TOTAL_AMT - TOTAL_AMT_PAID) total_remain,
	sum(TOTAL_AMT_PAID) total_paid,
	sum(PRINCIPAL - PRIN_PAID) prin_remain,
	sum(PRIN_PAID) prin_paid,
	sum(INTEREST - INT_PAID) int_remain,
	sum(INT_PAID) int_paid,
	sum(MTH_FEE - MTH_FEE_PAID) mth_remain,
	sum(MTH_FEE_PAID) mth_paid,
	sum(LPC - LPC_PAID) lpc_paid,
	sum(PENALTY_INT - PNLT_INT_PAID) penalty_paid,
	sum(OT_INT - OT_INT_PAID) ot_int_paid,
	sum(info_fee - info_fee_paid) info_remain,
	sum(info_fee_paid) info_paid,
	sum(add_fee1 - add_fee1_paid) fee1_remain,
	sum(add_fee1_paid) fee1_paid,
	sum(add_fee2- add_fee2_paid) fee2_remain,
	 sum(add_fee2_paid) fee2_paid,
 sum(add_fee3- add_fee3_paid) fee3_remain,
 sum(add_fee3_paid) fee3_paid,
sum(add_fee4- add_fee4_paid) fee4_remain,
sum(add_fee4_paid) fee4_paid
FROM
	fqmall_ht_prod.loan_repay_plan
GROUP BY
	LOAN_ID"""

loan_overdue_sql="""SELECT
	LOAN_ID loan_id,
	sum(TOTAL_AMT - TOTAL_AMT_PAID) total_behide,
	sum(PRINCIPAL - PRIN_PAID) prin_behind,
	sum(INTEREST - INT_PAID) int_behind,
	sum(MTH_FEE - MTH_FEE_PAID) mth_behid,
	sum(LPC - LPC_PAID) lpc_behind,
	sum(PENALTY_INT - PNLT_INT_PAID) penalty_behind,
	sum(OT_INT - OT_INT_PAID) ot_int_behind,
	sum(add_fee1 - add_fee1_paid) fee1_behind,
	sum(add_fee2 - add_fee2_paid) fee2_behind,
	sum(add_fee3 - add_fee3_paid) fee3_behind,
	sum(add_fee4 - add_fee4_paid) fee4_behind
FROM
	fqmall_ht_prod.loan_repay_plan
WHERE
	REPAY_PLAN_STATUS = 'REXP'
GROUP BY
	LOAN_ID"""

current_sql="""SELECT
	LOAN_ID loan_id,
	CURRENT_TERM current_term,
	PRINCIPAL prin_cur_topay,
	PRIN_PAID prin_cur_paid,
	INTEREST int_cur_topay,
	INT_PAID int_cur_paid,
	MTH_FEE mth_cur_topay,
	MTH_FEE_PAID mth_cur_paid,
	LPC lpc_cur_topay,
	LPC_PAID lpc_cur_paid,
	OT_FEE ot_cur_topay,
	OT_FEE_PAID ot_cur_paid,
	PENALTY_INT penalty_cur_topay,
	PNLT_INT_PAID penalty_cur_paid,
	TOTAL_AMT total_cur_topay,
	TOTAL_AMT_PAID total_cur_paid,
	OT_INT ot_int_cur_topay,
	OT_INT_PAID ot_int_cur_paid,
	info_fee info_cur_topay,
	info_fee_paid info_cur_paid,
	add_fee1 fee1_cur_topay,
	add_fee1_paid fee1_cur_paid,
	add_fee2 fee2_cur_topay,
	add_fee2_paid fee2_cur_paid,
	add_fee3 fee3_cur_topay,
	add_fee3_paid fee3_cur_paid,
	add_fee4 fee4_cur_topay,
	add_fee4_paid fee4_cur_paid
FROM
	fqmall_ht_prod.loan_repay_plan"""

case_info_sql="""SELECT pay_no,overdue_level,overdue_max_days overdue_days,finance from
ccms2.pu_case_info"""

loan_data=pd.read_sql(loan_sql,fq)

loan_repay_data=pd.read_sql(loan_repay_sql,fq)

loan_overdue_data=pd.read_sql(loan_overdue_sql,fq)

current_data=pd.read_sql(current_sql,fq)

case_info_data=pd.read_sql(case_info_sql,cc)


data1=pd.merge(loan_data,loan_repay_data,on='loan_id',how='left')

data2=pd.merge(data1,loan_overdue_data,on='loan_id',how='left')

data3=pd.merge(data2,current_data,on=['loan_id','current_term'],how='left')

data4=pd.merge(data3,case_info_data,left_on='application_no',right_on='pay_no',how='left').drop(['pay_no'],axis=1)

data4.to_sql('fact_photo_loan',dw,index=False,if_exists='append')
dw.close()
fq.close()
cc.close()
