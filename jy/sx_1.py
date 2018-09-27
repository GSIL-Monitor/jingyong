import pandas as pd
from sqlalchemy import create_engine
import datetime
import time
import pymysql
import numpy as np
from conn import *
fq=fq()
real=datetime.datetime.now().date()
today=datetime.datetime.now().date()-datetime.timedelta(days=1)


def step1():
    sql1="""SELECT a.cx_day,a.name,b.mobile as mobile_no,b.identity_no as iden,b.tag,b.user_type FROM
(SELECT DISTINCT NAME,IF(CONCAT(mobile)='',0,CONCAT(mobile)) mobile,IF(CONCAT(id_card_no)='',0,CONCAT(id_card_no)) iden,
DATE_SUB(DATE(create_time),INTERVAL 1 DAY) cx_day 
FROM dw_base.lose_credit WHERE DATE(create_time) BETWEEN '2018-06-19' AND '2018-06-27'
GROUP BY NAME,DATE_SUB(DATE(create_time),INTERVAL 1 DAY))a

LEFT JOIN

(SELECT a.mobile,a.identity_no,1 AS 'tag',CASE WHEN DATE(MAX(b.create_time))>='2018-03-01' THEN '老客' ELSE '新客' END AS 'user_type'
FROM dw_base.`dim_customer` a
LEFT JOIN fqmall_ht_prod.`loan_loan_acc` b
ON a.`user_name`=b.`USERNAME`
GROUP BY a.`user_name`
HAVING a.identity_no IS NOT NULL)b
ON a.mobile=b.mobile OR a.iden =b.identity_no
"""
    data1=pd.read_sql(sql1,fq)
    print(1)
    data1.columns=['cx_day','name','mobileno','iden','tag','user_type']
    data1.to_sql('lose22',dw,index=False,if_exists='append')
    print('原始数据处理、存储完成')



def step2():
    sql1="""
    SELECT a.name,a.mobileno,a.iden,a.cx_day,a.tag,b.type,c.scena FROM
    (SELECT name,mobileno,iden,cx_day,tag,user_type FROM dw_base.lose ) a
    LEFT JOIN 
    (
SELECT DISTINCT
	mobile_no,
	CASE
WHEN MAX(overdue_level) = 0 THEN
	'M0'
WHEN MAX(overdue_level) = 1 THEN
	'M1'
WHEN MAX(overdue_level) = 2 THEN
	'M2'
WHEN MAX(overdue_level) = 3 THEN
	'M3'
WHEN MAX(overdue_level) = 4 THEN
	'M4'
WHEN MAX(overdue_level) = 5 THEN
	'M5'
WHEN MAX(overdue_level) = 6 THEN
	'M6'
WHEN MAX(overdue_level) > 6 THEN
	'M7+'
END AS 'scena',
 identity_no
FROM
	ccms2.pu_case_info
GROUP BY
	mobile_no) c
    on a.mobileno=c.mobile_no or a.iden=c.identity_no
    LEFT JOIN
    (select DISTINCT cust_mobile,cust_certificate_no,CASE WHEN (sx_type is null and late_day >=4) then '失信' WHEN sx_type=1 then '诉讼' WHEN sx_type=2 THEN '仲裁'  END as 'type' 
    from fqmall_ht_prod.sx_data_copy) b
    on a.mobileno=b.cust_mobile or a.iden=b.cust_certificate_no
    """
    datas=pd.read_sql(sql1,fq)
    names=[]
    all_num=[]
    sys_num=[]
    sx=[]
    zc=[]
    ss=[]
    M0=[]
    M1=[]
    M2=[]
    M3=[]
    M4=[]
    M5=[]
    M6=[]
    M7=[]
    print('oooooooooooooooooooooooooooooooooooooooo')
    for name,data1 in datas.groupby(['cx_day']):
        names.append(name)
        all_num.append(len(data1.index))
        sys_num.append(len(data1[data1['tag']=='1'].index))
        print(name,all_num)
        sx.append(len(data1[data1['type']=='失信'].index))
        zc.append(len(data1[data1['type']=='仲裁'].index))
        ss.append(len(data1[data1['type']=='诉讼'].index))
        M0.append(len(data1[data1['scena']=='M0'].index))
        M1.append(len(data1[data1['scena']=='M1'].index))
        M2.append(len(data1[data1['scena']=='M2'].index))
        M3.append(len(data1[data1['scena']=='M3'].index))
        M4.append(len(data1[data1['scena']=='M4'].index))
        M5.append(len(data1[data1['scena']=='M5'].index))
        M6.append(len(data1[data1['scena']=='M6'].index))
        M7.append(len(data1[data1['scena']=='M7+'].index))

    fin=pd.DataFrame(data={'查询日期':names,'查询人数':all_num,'系统用户':sys_num,'失信':sx,'仲裁':zc,'诉讼':ss,'M0':M0,'M1':M1,'M2':M2,'M3':M3,'M4':M4,'M5':M5,'M6':M6,'M7+':M7})
    # finall=pd.concat(fin,axis=1)
    # fin.to_sql('lost_sys_detail',test,if_exists='append',index=False)
    print('失信信息查询存储完成')
    return fin



def step3():
    print('step3 start')
    sql2 = """SELECT a.cx_day,a.name,b.mobile,b.IDENTITY_NO,b.amount,c.type,b.user_type FROM
(SELECT NAME,iden,mobileno,cx_day FROM dw_base.lose WHERE cx_day BETWEEN '2018-06-19' AND '2018-06-27') a
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

    return data2s



if __name__=="__main__":
    step1()
    # fin=step2()
    # hk1,hk2=step3()
    # s1=pd.merge(fin,hk1,on=['查询日期'])
    # s2=pd.merge(s1,hk2,on=['查询日期'])
    # s2.to_sql('sx_all',test,index=False,if_exists='append')
    # fq.close()
    # dw.close()
    # cc.close()
    # test.close()
