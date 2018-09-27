import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
today=datetime.now().date()

cc=create_engine('mysql+pymysql://dw_dev:hydw@2018@10.253.169.47/ccms2?charset=utf8')
cc=cc.connect()
fq=create_engine('mysql+pymysql://dw_dev:hydw@2018@10.253.169.47/fqmall_ht_prod?charset=utf8')
fq=fq.connect()
dw=create_engine('mysql+pymysql://dw_dev:hydw@2018@10.253.169.47/dw_base?charset=utf8')
dw=dw.connect()

def step1():
    sql1="""SELECT DISTINCT name,IF(CONCAT(mobile)='',0,CONCAT(mobile)) mobile,IF(CONCAT(id_card_no)='',0,CONCAT(id_card_no)) iden,DATE_SUB(DATE(create_time),INTERVAL 1 DAY) cx_day from dw_base.lose_credit
    GROUP BY name,DATE_SUB(DATE(create_time),INTERVAL 1 DAY)"""
    sql2="""SELECT mobileno, '1' tag from cust_user"""
    sql3="""SELECT IDENTITY_NO, '1' tag2 from cust_customer"""

    data1=pd.read_sql(sql1,fq)
    data2=pd.read_sql(sql2,fq)
    data3=pd.read_sql(sql3,fq)
    print(1)
    data1['mobile']=data1['mobile'].astype(str)
    s1=pd.merge(data1,data2,left_on='mobile',right_on='mobileno',how='left').drop(['mobile'],axis=1)
    s2=pd.merge(s1,data3,left_on='iden',right_on='IDENTITY_NO',how='left').drop(['iden'],axis=1)

    nulls=s2['tag'].isnull()
    s2.loc[nulls,'tag']=s2.loc[nulls,'tag2']
    s2.drop_duplicates(inplace=True)
    s3=s2.drop(['tag2'],axis=1)
    print(s3)
    s3.columns=['name','cx_day','mobileno','tag','iden']
    s3.to_sql('lose',dw,index=False,if_exists='replace')


def step2():
    sql1="""
    SELECT a.name,a.mobileno,a.iden,a.cx_day,a.tag,b.type,c.scena FROM
    (SELECT name,mobileno,iden,cx_day,tag FROM dw_base.lose ) a
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
    from fqmall_ht_prod.sx_data) b
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
    cx_day=[]
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
    fin.to_sql('fin',dw,if_exists='replace',index=False)
    print(1)
    return fin

def step3():
    step2_data=step2()
    sql1 = """
    SELECT a.*,f.*,b.type FROM
    (SELECT NAME,iden,mobileno from dw_base.lose WHERE cx_day='%s' ) a
    INNER JOIN
    (SELECT sum(-a.AMOUNT) amount,DATE(a.CREATE_TIME) hk_day,c.MOBILE,c.IDENTITY_NO
    from acct_transaction a 
    LEFT JOIN loan_loan b
    on a.LOAN_ID=b.ID
    LEFT JOIN cust_customer c
    on b.USERNAME=c.USERNAME
    WHERE a.tran_type='PABK' and (DATE(a.CREATE_TIME) BETWEEN  DATE_SUB('%s',INTERVAL 8 DAY) and '%s')
    GROUP BY c.mobile,c.IDENTITY_NO,DATE(a.CREATE_TIME))f
    on (a.mobileno=f.mobile or a.iden=f.IDENTITY_NO)
    LEFT JOIN
    (select DISTINCT cust_mobile,cust_certificate_no,CASE WHEN (sx_type is null and late_day >=4) then '失信' WHEN sx_type=1 then '诉讼' WHEN sx_type=2 THEN '仲裁'  END as 'type' 
    from fqmall_ht_prod.sx_data) b
    on a.mobileno=b.cust_mobile or a.iden=b.cust_certificate_no"""%(today,today,today)
    data1 = pd.read_sql(sql1, fq)
    data2 = data1[['hk_day', 'amount', 'type']]
    data3 = pd.pivot_table(data2, values='amount', index='hk_day', aggfunc=('sum', 'count'))
    data3.columns=['累计还款人数','累计还款金额']
    data4=pd.merge(step2_data,data3,left_on='查询日期',right_index=True,how='left')
    data5 = pd.pivot_table(data2, columns='type', index=['hk_day'], aggfunc=('count'))
    data6=pd.merge(data4,data5,left_on='查询日期',right_index=True,how='left')
    print(data6)
    data6.to_sql('finsss',dw,index=False,if_exists='replace')

if __name__=="__main__":
    # step1()
    # step2()
    step3()