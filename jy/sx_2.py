import pandas as pd
from conn import *
import numpy as np
import datetime
import pymysql
fq=fq()
sx=sx()
dw=dw()

conn3 = pymysql.connect(
    host="10.253.169.47",
    user="otter",
    passwd="otter@123",
    charset="utf8",
    db='sx_info',
    use_unicode=False
)
cur3=conn3.cursor()


def step1(today):
    sql1="""SELECT a.cx_day,a.name,b.mobile as mobile_no,b.identity_no as iden,b.tag,b.user_type FROM
(SELECT DISTINCT NAME,IF(CONCAT(mobile)='',0,CONCAT(mobile)) mobile,IF(CONCAT(id_card_no)='',0,CONCAT(id_card_no)) iden,
DATE_SUB(DATE(create_time),INTERVAL 1 DAY) cx_day 
FROM dw_base.lose_credit WHERE DATE(create_time) ='%s'
GROUP BY NAME,DATE_SUB(DATE(create_time),INTERVAL 1 DAY))a

LEFT JOIN

(SELECT a.mobile,a.identity_no,1 AS 'tag',CASE WHEN DATE(MAX(b.create_time))>='2018-03-01' THEN '老客' ELSE '新客' END AS 'user_type'
FROM dw_base.`dim_customer` a
LEFT JOIN fqmall_ht_prod.`loan_loan_acc` b
ON a.`user_name`=b.`USERNAME`
GROUP BY a.`user_name`
HAVING a.identity_no IS NOT NULL)b
ON a.mobile=b.mobile OR a.iden =b.identity_no
"""%today
    data1=pd.read_sql(sql1,fq)

    print(1)

    data1.columns=['cx_day','name','mobileno','iden','tag','user_type']
    data1.to_sql('lose1',dw,index=False,if_exists='append')
    print('原始数据处理、存储完成')


def step2(yes):
    sql3="""
        SELECT name,mobileno,iden,cx_day,tag,user_type FROM dw_base.lose1 WHERE cx_day  ='%s'
        """%yes
    data3=pd.read_sql(sql3,fq)

    sql1="""SELECT DISTINCT
        mobile_no as mobileno,
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
    identity_no as iden
    FROM
        ccms2.pu_case_info
    GROUP BY
        mobile_no"""
    data1=pd.read_sql(sql1,fq)

    sql2="""select DISTINCT cust_mobile as mobileno,cust_certificate_no as iden,CASE WHEN (sx_type is null and late_day >=4) then '失信' WHEN sx_type=1 then '诉讼' WHEN sx_type=2 THEN '仲裁'   END as 'type' 
        from fqmall_ht_prod.sx_data_yes"""
    data2=pd.read_sql(sql2,fq)


    data1['mobileno']=data1['mobileno'].astype('str')
    data2['mobileno']=data2['mobileno'].astype('str')
    data3['mobileno']=data3['mobileno'].astype('str')
    data1['iden']=data1['iden'].astype('str')
    data2['iden']=data2['iden'].astype('str')
    data3['iden']=data3['iden'].astype('str')

    data4=pd.merge(data3,data1,on='mobileno',how='left').drop(['iden_y'],axis=1)
    data5=pd.merge(data4,data2,on='mobileno',how='left').drop(['iden'],axis=1)
    data6=pd.merge(data5,data1,left_on='iden_x',right_on='iden',how='left').drop(['mobileno_y','iden'],axis=1)
    data7=pd.merge(data6,data2,left_on='iden_x',right_on='iden',how='left').drop(['mobileno','iden'],axis=1)
    nulls=data7['scena_x'].isnull()
    data7.loc[nulls,'scena_x']=data7.loc[nulls,'scena_y']
    nulls=data7['type_x'].isnull()
    data7.loc[nulls,'type_x']=data7.loc[nulls,'type_y']
    data8=data7.drop(['scena_y','type_y'],axis=1)
    data8.drop_duplicates(inplace=True)

    data9=data8.pivot_table(index='cx_day',columns=['user_type'],values='name',aggfunc=np.size).reset_index()
    data10=data8.pivot_table(index='cx_day',columns=['scena_x'],values='name',aggfunc=np.size).reset_index()
    data11=data8.pivot_table(index='cx_day',columns=['type_x'],values='name',aggfunc=np.size).reset_index()


    sql13="""SELECT DATE_SUB(DATE(create_time),INTERVAL 1 DAY) cx_day,COUNT(*) num from dw_base.lose_credit 
        WHERE (DATE_SUB(DATE(create_time),INTERVAL 1 DAY) = '%s') GROUP BY (DATE(create_time))"""%yes
    data13=pd.read_sql(sql13,dw)
    data14=pd.concat([data9,data10,data11,data13],axis=1)
    print(data14.columns)

    data14.columns=['cx_day','新客','老客','cx_day2','MO','M1','M2','M3','M4','M5','M6','M7+','cx_day3','仲裁','失信','诉讼','cx_day4','查询次数']

    data14.drop(['cx_day2','cx_day3','cx_day4'],axis=1,inplace=True)
    data14['查询人数']=len(data3.index)

    data14.to_sql('lose2',dw,if_exists='append',index=False)
    print('step2 done!!')





def step3(day,today):
    sql1 = """SELECT NAME,iden,mobileno,cx_day FROM dw_base.lose1 WHERE cx_day ='%s' """%day
    data1 = pd.read_sql(sql1, fq)
    sql2 = """SELECT SUM(-a.AMOUNT) amount,c.MOBILE,c.IDENTITY_NO,CASE WHEN DATE(MAX(b.create_time))>='2018-03-01' THEN '老客' ELSE '新客' END AS 'user_type'
    FROM acct_transaction a 
    LEFT JOIN loan_loan b
    ON a.LOAN_ID=b.ID
    LEFT JOIN cust_customer c
    ON b.USERNAME=c.USERNAME
    WHERE a.tran_type='PABK' AND (DATE(a.CREATE_TIME) BETWEEN '%s' AND '%s')
    GROUP BY c.IDENTITY_NO"""%(day,today)
    data2 = pd.read_sql(sql2, fq)
    data2.drop_duplicates(inplace=True)
    sql3 = """select DISTINCT cust_mobile as mobileno,cust_certificate_no as iden,CASE WHEN (sx_type is null and late_day >=4) then '失信' WHEN sx_type=1 then '诉讼' WHEN sx_type=2 THEN '仲裁'   END as 'type' 
        from fqmall_ht_prod.sx_data"""
    data3 = pd.read_sql(sql3, fq)
    data4 = pd.merge(data2, data3, left_on='IDENTITY_NO', right_on='iden', how='left')
    data5 = pd.merge(data1, data4, left_on='mobileno', right_on='MOBILE', how='left')
    data6 = pd.merge(data5, data4, left_on='iden_x', right_on='IDENTITY_NO', how='left')
    nulls = data6['user_type_x'].isnull()
    data6.loc[nulls, 'user_type_x'] = data6.loc[nulls, 'user_type_y']
    nulls = data6['type_x'].isnull()
    data6.loc[nulls, 'type_x'] = data6.loc[nulls, 'type_y']
    nulls = data6['amount_x'].isnull()
    data6.loc[nulls, 'amount_x'] = data6.loc[nulls, 'amount_y']
    data6.drop(
        ['iden_x', 'mobileno_x', 'type_y', 'IDENTITY_NO_x', 'MOBILE_x', 'mobileno_y', 'iden_y', 'amount_y', 'MOBILE_y',
         'IDENTITY_NO_y', 'user_type_y'], axis=1, inplace=True)
    data6.columns = ['name', 'cx_day', 'amount', 'user_type', 'type', 'mobileno', 'iden']
    data6.drop_duplicates(inplace=True)
    data7 = data6.pivot_table(index='cx_day', columns='user_type', values='amount', aggfunc=np.size).reset_index()
    data8 = data6.pivot_table(index='cx_day', columns='type', values='amount', aggfunc=np.size).reset_index().drop(
        ['cx_day'], axis=1)
    data9 = pd.concat([data7, data8], axis=1)
    data9['还款金额'] = data6['amount'].fillna(0).sum()
    # print(data9.columns)
    return data9


def step4(s_data3):
    data1=pd.read_sql("""select * from lose2""",dw)
    data2=pd.merge(s_data3,data1,on='cx_day',how='left')
    ids = "','".join(['%s' for _ in range(len(data2['cx_day']))])

    del_sql="""delete from sx_pay where cx_day IN ('%s')"""% (ids) % tuple(data2['cx_day'].tolist())

    cur3.execute(del_sql)
    conn3.commit()
    data2.to_sql('sx_pay', sx, index=False, if_exists='append')




if __name__=="__main__":
    today = datetime.datetime.now().date()
    yestrday=today-datetime.timedelta(days=1)
    # today = '2018-07-02'
    # yestrday = '2018-07-01'
    day_list=pd.date_range(end='%s'%yestrday,freq='D',periods=9).tolist()
    day_list2=pd.date_range(end='%s'%today,freq='D',periods=9).tolist()
    payback=[]

    print('step1.............................')
    step1(today)
    # for i in day_list2:
    #     print(i)
    # step2(yestrday)

    print('step2..............................')
    step2(yestrday)
    print('step3..............................')
    for i in day_list:
        print(i)
        payback.append(step3(i,today))
    s_data3=pd.concat(payback)
    s_data3.columns=['cx_day','仲裁还款人数','失信还款人数','新客还款人数','老客还款人数','诉讼还款人数','还款金额']
    s_data3.to_sql('lose3',sx,index=False,if_exists='replace')
    step4(s_data3)
    conn_close()
    cur3.close()
    conn3.close()
    # print(day_list)


