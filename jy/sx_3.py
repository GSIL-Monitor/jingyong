import pandas as pd
from conn import *
import numpy as np
import datetime
fq=fq()
sx=sx()


def step3():
    print('step3 start')
    sql2 = """SELECT a.cx_day,a.name,b.mobile,b.IDENTITY_NO,b.amount,c.type,b.user_type FROM
    (SELECT NAME,iden,mobileno,cx_day FROM dw_base.lose22 WHERE cx_day BETWEEN '2018-06-19' AND '2018-06-27') a
    INNER JOIN
    (SELECT SUM(-a.AMOUNT) amount,c.MOBILE,c.IDENTITY_NO,CASE WHEN DATE(MAX(b.create_time))>='2018-03-01' THEN '老客' ELSE '新客' END AS 'user_type'
    FROM acct_transaction a 
    LEFT JOIN loan_loan b
    ON a.LOAN_ID=b.ID
    LEFT JOIN cust_customer c
    ON b.USERNAME=c.USERNAME
    WHERE a.tran_type='PABK' AND (DATE(a.CREATE_TIME) BETWEEN '2018-06-19' AND '2018-06-27')
    GROUP BY c.mobile,c.IDENTITY_NO) b
    ON (a.mobileno = b.mobile OR a.iden=b.IDENTITY_NO) 
    LEFT JOIN
    (SELECT DISTINCT cust_mobile,cust_certificate_no,CASE WHEN (sx_type IS NULL AND late_day >=4) THEN '失信' WHEN sx_type=1 THEN '诉讼' WHEN sx_type=2 THEN '仲裁'  END AS 'type'
    FROM fqmall_ht_prod.sx_data_yes) c
    ON a.mobileno=c.cust_mobile OR a.iden=c.cust_certificate_no"""

    data1 = pd.read_sql(sql2, fq)
    data2 = data1.drop(['type'], axis=1)
    data2.drop_duplicates(inplace=True)
    data3 = data2.pivot_table(index='cx_day', columns='user_type', values='amount',aggfunc=(np.sum, np.size)).reset_index()

    data1s = data1[data1['type'].isin(['失信', '诉讼', '仲裁'])]
    data2s = data1s.pivot_table(index='cx_day', columns='type', values='amount', aggfunc=np.size).reset_index()
    data3s = pd.merge(data2s, data3, on='cx_day', how='left')

    name_dict = {'cx_day': '查询日期', '仲裁': '仲裁还款人数',
                 '失信': '失信还款人数', '诉讼': '诉讼还款人数',
                 'size,新客': '新客还款人数','size,老客': '老客还款人数',
                 'sum,新客': '新客还款金额', 'sum,老客': '老客还款人数'}

    data2s.rename(name_dict, axis=1,inplace=True)

    data2s.to_sql('sx_all2_1',dw(),index=False,if_exists='replace')
    conn_close()

step3()


