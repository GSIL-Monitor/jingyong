import pandas as pd
from sqlalchemy import create_engine

cc=create_engine('mysql+pymysql://dw_dev:hydw@2018@10.253.169.47/ccms2?charset=utf8')
cc=cc.connect()
fq=create_engine('mysql+pymysql://dw_dev:hydw@2018@10.253.169.47/fqmall_ht_prod?charset=utf8')
fq=fq.connect()
dw=create_engine('mysql+pymysql://dw_dev:hydw@2018@10.253.169.47/dw_base?charset=utf8')
dw=dw.connect()


sql1="""
SELECT
	a.APPLICATION_ID '亨元申请编号',
	e.name '姓名',
	e.identity_no '身份证',
	b.AVAILABLE_DATE '建账日期',
	a.AMOUNT '合同金额',
	b.TOTAL_TERM '借款期数',
	c.overdue_level '逾期阶段',
	d.`NAME` '产品',
    b.loan_id,
    CASE WHEN d.investor_name='beixiao_finance' THEN '北银' WHEN d.investor_name='haier_finance' THEN '海尔' 
WHEN d.investor_name='huarong_finance' THEN '华融'  WHEN d.investor_name='face_finance' THEN '笑脸' 
WHEN d.investor_name='dfs_finance' THEN '大丰收'  WHEN d.investor_name='koudai_finance' THEN '口袋' 
WHEN d.investor_name='xiaonuo_finance' THEN '小诺'  WHEN d.investor_name='jinshang_finance' THEN '晋商' 
WHEN d.investor_name='djs_finance' THEN '点金石'  WHEN d.investor_name='yingyan_finance' THEN '盈衍'
WHEN d.investor_name='david_fu' THEN '自有' end as '资金方'
    
FROM
	loan_loan a
LEFT JOIN loan_loan_acc b ON a.ID = b.LOAN_ID
LEFT JOIN ccms2.pu_case_info c ON a.APPLICATION_ID = c.pay_no
LEFT JOIN loan_product d on a.PRODUCT_ID=d.ID
left JOIN cust_customer e on a.username=e.username
WHERE c.overdue_level>0"""

data1=pd.read_sql_query(sql1,fq)
data1['loan_id']=data1['loan_id'].astype(int)

benjin_sql="""SELECT loan_id,(SUM(PRINCIPAL)-SUM(PRIN_PAID)) '本金余额' FROM loan_repay_plan GROUP BY LOAN_ID"""
benjin_data=pd.read_sql(benjin_sql,fq)
benjin_data['loan_id']=benjin_data['loan_id'].astype(int)

data2=pd.merge(data1,benjin_data,on='loan_id',how='left')
data2['loan_id']=data2['loan_id'].astype(int)

yihuan_num_sql="""SELECT loan_id,MAX(current_term) '已还期数' FROM loan_repay_plan WHERE TOTAL_AMT=TOTAL_AMT_PAID
GROUP BY loan_id;"""
yihuan_day=pd.read_sql(yihuan_num_sql,fq)
yihuan_day['loan_id']=yihuan_day['loan_id'].astype(int)

data3=pd.merge(data2,yihuan_day,on='loan_id',how='left')

zijinfang_sql="""#合同编号及当前资金方
#--华融
SELECT DISTINCT hy_application_no,external_contract_no '资金方合同号','华融'AS'放款资金方'
from inv_huarong_application
UNION
SELECT hy_application_no,''AS '资金方合同号','晋商'AS '资金方'
from inv_jinshang_application 
UNION
SELECT hy_application_no,loan_release_batch_no AS '资金方合同号','笑脸' AS'放款资金方'
from inv_facebank_application 
UNION
#--口袋
SELECT hy_application_no,''AS '资金方合同号','口袋' AS'放款资金方'
from inv_koudai_application
UNION
#--小诺
SELECT hy_application_no,'' AS'资金方合同号','小诺' AS'放款资金方'
from `inv_xiaonuo_application`
UNION
#--汇有财
SELECT hy_application_no,''AS '资金方合同号','汇有财' AS'放款资金方'
FROM `inv_dianjinshi_application`
UNION
#--盈衍
SELECT hy_application_no,''AS '资金方合同号','盈衍'AS '放款资金方'
FROM `inv_yingyan_application`
UNION
#--海尔
SELECT hy_application_no,cont_No AS'资金方合同号','海尔'AS '放款资金方'
FROM he_application_pay

UNION
#--广群
SELECT hy_application_no,''AS '资金方合同号','广群'AS '放款资金方'
from `inv_guangqun_application`
UNION

SELECT hy_application_no,'' AS'资金方合同号','自有' AS'放款资金方'
from inv_hy_application"""
zijin_data=pd.read_sql(zijinfang_sql,fq)
zijin_data['hy_application_no']=zijin_data['hy_application_no'].astype('str')
data3['亨元申请编号']=data3['亨元申请编号'].astype('str')


dakuan_sql="""SELECT
	a.app_No,
	b.BANK_NAME '打款银行',
	TOCASH_TYPE '提现方式',
	c.DIC_NAME '打款方式'
FROM
	cust_tocash a
LEFT JOIN hy_bank b on a.TC_BANK_CODE= b.BANK_CODE
LEFT JOIN d_dit_dic c on a.TOCASH_TYPE=c.dic_code"""
dakuan_data=pd.read_sql_query(dakuan_sql,fq)


data4=pd.merge(data3,zijin_data,left_on='亨元申请编号',right_on='hy_application_no',how='left').drop(['loan_id','hy_application_no'],axis=1)

data4['未还期数']=data4['借款期数']-data4['已还期数']

dakuan_sql="""SELECT
	a.app_No,
	b.BANK_NAME '打款银行',
	TOCASH_TYPE '提现方式',
	c.DIC_NAME '打款方式'
FROM
	cust_tocash a
LEFT JOIN hy_bank b on a.TC_BANK_CODE= b.BANK_CODE
LEFT JOIN d_dit_dic c on a.TOCASH_TYPE=c.dic_code"""
dakuan_data=pd.read_sql_query(dakuan_sql,fq)

dakuan_data['app_No']=dakuan_data['app_No'].astype('str')
data4=pd.merge(data4,dakuan_data,left_on='亨元申请编号',right_on='app_No',how='left').drop(['app_No'],axis=1)
data4.to_excel('d:\\资金方全量表2.xlsx',index=False)
print(data4.head())