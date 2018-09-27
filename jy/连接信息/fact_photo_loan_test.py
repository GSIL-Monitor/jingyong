import pandas as pd
from sqlalchemy import create_engine
import datetime
from get_config import get_config
from sms import sendto as cs
import gc
import time
from duoyuan_conn import *
t1=time.clock()
# cs(['17721291792','15822708861','18301959085'],content='快照表定时任务开始')
connFq = dfq()
connDw = ddw()
connCc = dcc()
# connCc_test= create_engine('mysql+pymysql://{}:{}@{}/{}?charset=utf8'.format(*get_config('ccms_test')))
print("loading loan_loan_acc....")
now_time = datetime.datetime.now()
yes_time = now_time + datetime.timedelta(days=-1)
yes_time = yes_time.date()
now_time = now_time.date()

#订单是否结束，1表示结束
is_end_sql = '''
        select loan_id, 1 as is_end from loan_loan_acc where next_stmt_date is null and loan_status='N'
'''

end_data = pd.read_sql(is_end_sql, connFq)

#取出所有订单，作为基表
loan_sql = """
            SELECT ll.ID loan_id, ll.APPLICATION_ID application_no,
            llc.CUR_TERM cur_term, llc.create_time
            FROM 
            loan_loan_acc llc 
            LEFT JOIN loan_loan ll ON ll.ID = llc.LOAN_ID
            WHERE 
            llc.create_time < '{}'
"""
loan_data = pd.read_sql(loan_sql.format(now_time), connFq)

print(len(loan_data))
print("loading loan_loan_acc Done")

# 设置是否结束

loan_data = pd.merge(loan_data, end_data, on='loan_id', how='left')

#未结束的设置为0
loan_data['is_end'].fillna(0, inplace=True)


# 设置正确的当前期数
repay_sql = '''
    select distinct loan_id, current_term from loan_repay_plan where due_date = '{}' 
'''

repay_daa = pd.read_sql(repay_sql.format(yes_time), connFq)

loan_data = pd.merge(loan_data, repay_daa, on='loan_id', how='left')


#反过来用cur_term 去补current_term
loan_data.current_term.fillna(loan_data.cur_term, inplace=True)
loan_data.drop(['cur_term'], axis=1, inplace=True)
print(len(loan_data))

# 设置sum
print("loading loan_sum_sql...")
loan_sum_sql = """
                SELECT LOAN_ID loan_id, sum(TOTAL_AMT - TOTAL_AMT_PAID) total_remain, sum(TOTAL_AMT_PAID) total_paid, 
                sum(PRINCIPAL - PRIN_PAID) prin_remain, sum(PRIN_PAID) prin_paid, 
                sum(INTEREST - INT_PAID) int_remain, sum(INT_PAID) int_paid, 
                sum(MTH_FEE - MTH_FEE_PAID) mth_remain, sum(MTH_FEE_PAID) mth_paid, 
                sum(LPC_PAID) lpc_paid, sum(PNLT_INT_PAID) penalty_paid, 
                sum(OT_INT_PAID) ot_int_paid, sum(info_fee - info_fee_paid) info_remain, 
                sum(info_fee_paid) info_paid, sum(add_fee1 - add_fee1_paid) fee1_remain, 
                sum(add_fee1_paid) fee1_paid, sum(add_fee2- add_fee2_paid) fee2_remain,
                sum(add_fee2_paid) fee2_paid, sum(add_fee3- add_fee3_paid) fee3_remain, 
                sum(add_fee3_paid) fee3_paid, sum(add_fee4- add_fee4_paid) fee4_remain, 
                sum(add_fee4_paid) fee4_paid FROM loan_repay_plan GROUP BY LOAN_ID
                """
loan_sum_data = pd.read_sql(loan_sum_sql, connFq)

data_loan_sum = pd.merge(loan_data, loan_sum_data, on='loan_id', how='left')

print(len(data_loan_sum))
del loan_sum_data, loan_data
gc.collect()
print("loading loan_sum_sql Done")

# 设置逾期sum   REXP=逾期
print("loading loan_overdue_sql ...")
loan_overdue_sql = """
                SELECT LOAN_ID loan_id, sum(TOTAL_AMT - TOTAL_AMT_PAID) total_behide, sum(PRINCIPAL - PRIN_PAID) prin_behind, 
                sum(INTEREST - INT_PAID) int_behind, sum(MTH_FEE - MTH_FEE_PAID) mth_behid, sum(LPC - LPC_PAID) lpc_behind, 
                sum(PENALTY_INT - PNLT_INT_PAID) penalty_behind, sum(OT_INT - OT_INT_PAID) ot_int_behind,
                sum(add_fee1 - add_fee1_paid) fee1_behind, sum(add_fee1 - add_fee1_paid) fee1_behind,
                sum(add_fee2 - add_fee2_paid) fee2_behind, sum(add_fee3 - add_fee3_paid) fee3_behind, 
                sum(add_fee4 - add_fee4_paid) fee4_behind FROM loan_repay_plan WHERE
                REPAY_PLAN_STATUS = 'REXP' GROUP BY LOAN_ID
                """
loan_overdue_data = pd.read_sql(loan_overdue_sql, connFq)

data_loan_overdue = pd.merge(data_loan_sum, loan_overdue_data, on='loan_id', how='left')

print(len(data_loan_overdue))
del data_loan_sum, loan_overdue_data
gc.collect()
print("loading loan_overdue_sql Done")

# 设置本期
current_sql = """
              SELECT distinct LOAN_ID loan_id, NOW() create_date, CURRENT_TERM current_term, PRINCIPAL prin_cur_topay,
              PRIN_PAID prin_cur_paid, INTEREST int_cur_topay, INT_PAID int_cur_paid, MTH_FEE mth_cur_topay, 
              MTH_FEE_PAID mth_cur_paid, LPC lpc_cur_topay, LPC_PAID lpc_cur_paid, OT_FEE ot_cur_topay,
              OT_FEE_PAID ot_cur_paid, PENALTY_INT penalty_cur_topay, PNLT_INT_PAID penalty_cur_paid, 
              TOTAL_AMT total_cur_topay, TOTAL_AMT_PAID total_cur_paid, OT_INT ot_int_cur_topay, 
              OT_INT_PAID ot_int_cur_paid, info_fee info_cur_topay, info_fee_paid info_cur_paid, 
              add_fee1 fee1_cur_topay, add_fee1_paid fee1_cur_paid, add_fee2 fee2_cur_topay,
              add_fee2_paid fee2_cur_paid, add_fee3 fee3_cur_topay, add_fee3_paid fee3_cur_paid, 
              add_fee4 fee4_cur_topay, add_fee4_paid fee4_cur_paid, due_date
              FROM loan_repay_plan 
              WHERE  LOAN_ID in ({})
              """
print("loading current_sql ...")
id_list = list(data_loan_overdue['loan_id'])
id_str = ','.join(([str(int(i)) for i in id_list]))
current_data = pd.read_sql(current_sql.format(id_str), connFq)
data_current = pd.merge(data_loan_overdue, current_data, on=['loan_id', 'current_term'], how='left')

print(len(data_current))
del data_loan_overdue, current_data
gc.collect()
print("loading current_sql Done")

# 设置逾期阶段
case_info_sql = """
            SELECT
	`a`.`LOAN_ID` AS loan_id,
	(CASE WHEN ( `a`.`NEXT_STMT_DATE` IS NOT NULL ) THEN
	 			((max( `b`.`CURRENT_TERM` ) - min( `b`.`CURRENT_TERM` )) + 1 
) ELSE ((
						(max( `b`.`CURRENT_TERM` ) - min( `b`.`CURRENT_TERM` )) + 1 ) + floor(
						((to_days( now( ) ) - to_days( `a`.`LAST_STMT_DATE` )) / 30) 
					)) END) AS `overdue_level`,
			(
			to_days( now( ) ) - to_days( min( `b`.`DUE_DATE`))
			) AS `overdue_days`
			
			FROM
				 fqmall_ht_prod.`loan_loan_acc` `a` 
			JOIN fqmall_ht_prod.`loan_repay_plan` `b`
			WHERE
					 `a`.`LOAN_ID` = `b`.`LOAN_ID` 
			GROUP BY
			`a`.`LOAN_ID` 
            """
print("loading case_info_sql ...")
# app_list = list(data_current['application_no'])
# app_str = ','.join(([str(int(i)) for i in app_list]))
case_info_data = pd.read_sql(case_info_sql, connFq)
data_case_info = pd.merge(data_current, case_info_data, on='loan_id',how='left')
# data_case_info['overdue_level'].fillna(0,inplace=True)
# data_case_info['overdue_days'].fillna(0,inplace=True)

print(len(data_case_info))
del data_current, case_info_data
gc.collect()
print("loading case_info_sql Done")

# 设置新老客户，VIP贷
application_sql = '''
    SELECT app_application_no, hy_industry_code, app_username FROM d_application_pay where 
    app_application_no in ({})
'''

print("loading application ...")
application_data = pd.read_sql(application_sql.format(app_str), connFq)
data_application = pd.merge(data_case_info, application_data, left_on='application_no',
                            right_on='app_application_no', how='left').drop(['app_application_no'], axis=1)

print("loading application Done")
print(len(data_application))

product_sql = '''
        select product_type_key as user_type, product_code from dim_product_type
'''
print("loading product_sql ...")
product_data = pd.read_sql(product_sql, connDw)
data_product = pd.merge(data_application, product_data, left_on='hy_industry_code',
                        right_on='product_code', how='left')
print("loading product_sql Done")
print(len(data_product))

# 关联身份证号
cust_sql = '''
    select username, IDENTITY_NO from cust_customer
'''
cust_data = pd.read_sql(cust_sql, connFq)
data_product = pd.merge(data_product, cust_data, left_on='app_username',
                        right_on='username', how='left')


#取出老客户的key
MDOH_key = product_data[product_data['product_code'] == 'MDOH'].iloc[0, 0]


#MDCP 新客户  MDOH 老客户

# 按身份证取重复行
data_product.sort_values(by=['create_time'], axis=0, ascending=True, inplace=True)


#duplicated 表示标记后面的全部标记为重复，第一个不算
duplicated = (data_product.IDENTITY_NO.duplicated(keep='first')) & (data_product['product_code'] == 'MDCP')
data_product.loc[duplicated, 'user_type'] = MDOH_key

data_product.drop(['hy_industry_code', 'product_code', 'IDENTITY_NO', 'app_username', 'username'], axis=1, inplace=True)

print('MDOH_key  Done')
print(len(data_product))


# 写入数据库
print('writing to database...')
data_product.fillna(0, inplace=True)
data_product['finance']=None
data_product.to_sql('fact_photo_loan', connDw, index=False, if_exists='append', chunksize=30000)
print('writing to database Done')
t2=time.clock()
print(t2-t1)
# cs(['17721291792','15822708861','18301959085'],content='快照表在{}完成'.format(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S')))
