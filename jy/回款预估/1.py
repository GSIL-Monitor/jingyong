import pandas as pd
from sqlalchemy import create_engine
import pymysql
import numpy as np


conn=create_engine('mysql+pymysql://huitongfq_prod:hy-123456.com@121.43.73.95:2800/fqmall_ht_prod?charset=utf8')
cur=conn.connect()
yingshou_sql="""SELECT 
LEFT(p.DUE_DATE,7) '年月',
m.capitalside '资金方',
(CASE 
WHEN hip.`HY_CREDIT_NAME` ='嗨秒贷' AND m.`available_date`>'2016-12-31' THEN '新秒贷' 
WHEN m.`hy_industry_code`='DDCP' AND m.available_date<'2017-08-02' THEN '滴滴贷'
WHEN m.`salecenter`='I换' AND m.`hy_industry_code`<>'HINS' THEN '嗨数贷-I换'
ELSE hip.`HY_CREDIT_NAME` END) AS 产品名称,
 RIGHT(p.DUE_DATE,2) 'cycle', 
 (CASE WHEN IFNULL(m.`collectionM`,0)>6 THEN '7+' ELSE IFNULL(m.`collectionM`,0) END ) '逾期阶段',
 dap.`APPLY_AMOUNT`  '批复金额',	
(CASE WHEN m.`salecenter`='I换' AND m.`hy_industry_code`<>'HINS' THEN ''
WHEN  m.`available_date`>'2016-12-31' THEN '新' 
WHEN hip.`HY_CREDIT_NAME`='滴答贷' THEN '新'

ELSE '旧' END) AS '新/旧',
 SUM(p.TOTAL_AMT) '总金额', 
 SUM(p.`PRINCIPAL`) '本金',
 SUM(p.`INTEREST`) '利息',
 SUM(p.`LPC`) '滞纳金',
 SUM(p.`PENALTY_INT`) '罚息',
 SUM(p.MTH_FEE) '账户管理费',
sum(p.`info_fee`) '信息服务费',
sum(p.`add_fee1`+ p.`add_fee2`) '保险费',
'当月应收' 来源
#sum(p.`OT_FEE` )'其他费用',
#sum(p.`add_fee3`) '附加费3',
#sum(p.`add_fee4`) '附加费4'
FROM fqmall_ht_prod.loan_repay_plan p
INNER JOIN mis_loan_detail m ON p.LOAN_ID = m.loan_id
LEFT JOIN d_application_pay dap ON dap.`app_application_no` = m.`applicationno`
LEFT JOIN hy_industry_param hip ON hip.HY_INDUSTRY_CODE = m.hy_industry_code
WHERE (p.DUE_DATE BETWEEN '2018-01-01' and '2018-05-31') AND p.CURRENT_TERM<>0
AND IFNULL(m.iscancel,'') <> 'Y'
AND  (m.`principal_ys`- m.`principal_paid`) <>0
GROUP BY 年月, 资金方, 产品名称, cycle,逾期阶段,批复金额,'新/旧'"""

over_due_sql="""SELECT
m.`month` 自然月,
LEFT(m.`available_date`,7) 放贷月,
(CASE WHEN IFNULL(m.`capitalside`,'')='beixiao_finance' THEN '北销'
WHEN IFNULL(m.`capitalside`,'')='dfs_finance' THEN '大丰收'
WHEN IFNULL(m.`capitalside`,'')='face_finance' THEN '笑脸'
WHEN IFNULL(m.`capitalside`,'')='guangqun_finance' THEN '广群'
WHEN IFNULL(m.`capitalside`,'')='haier_finance' THEN '海尔'
WHEN IFNULL(m.`capitalside`,'')='huarong_finance' THEN '华融'
WHEN IFNULL(m.`capitalside`,'')='jinshang_finance' THEN '晋商'
WHEN IFNULL(m.`capitalside`,'')='koudai_finance' THEN '口袋'
WHEN IFNULL(m.`capitalside`,'')='xiaonuo_finance' THEN '小诺'
WHEN IFNULL(m.`capitalside`,'')='' THEN '自有'
ELSE m.`capitalside` END) 资金方,
(CASE WHEN hip.`HY_CREDIT_NAME` ='嗨秒贷' AND m.`available_date`>'2016-12-31' THEN '新秒贷' 	
WHEN m.`hy_industry_code`='DDCP' AND m.available_date<'2017-08-02' THEN '滴滴贷'	
WHEN m.`salecenter`='I换' AND m.`hy_industry_code`<>'HINS' THEN '嗨数贷-I换'	
ELSE hip.`HY_CREDIT_NAME` END) AS 产品,
m.`totalterm` 总期数,
(CASE WHEN IFNULL(m.`collectionM`,0)>=7 THEN '7+' ELSE IFNULL(m.`collectionM`,0) END)逾期阶段,
SUM(m.`contractamt`) 总本金,
SUM(m.`loan_balance`)'余额-本息',
SUM(m.`principal_ys`+m.`service_fee_ys`-m.`principal_paid`-m.`service_fee_paid`) '余额-本金',
SUM(m.`delq_principal`+m.`delq_servicefee`) 到期本金,
SUM(m.`balance`) 到期本息,
COUNT(*)
FROM mis_loan_detail_his m 
INNER JOIN hy_industry_param hip ON  hip.`HY_INDUSTRY_CODE`=m.`hy_industry_code`
WHERE IFNULL(m.`iscancel`,'')<>'Y' AND IFNULL(m.`app_sale_code`,'')<>'9001' 
AND m.`month` BETWEEN '2018-01' AND '2018-05' 
AND DATE(m.`available_date`)<DATE(NOW())
AND (m.`principal_ys`+m.`service_fee_ys`-m.`principal_paid`-m.`service_fee_paid`)>0
GROUP BY 自然月,放贷月,资金方,产品,总期数,逾期阶段"""




yingshou_data=pd.read_sql(yingshou_sql,conn)
new_yingshou=yingshou_data[yingshou_data['新/旧']=='新']
old_yingshou=yingshou_data[yingshou_data['新/旧']=='旧']
data= pd.pivot_table(new_yingshou,values='总金额',index=['年月','产品名称','逾期阶段'],aggfunc=np.sum)
data2= pd.pivot_table(old_yingshou,values='总金额',index=['年月','产品名称','逾期阶段'],aggfunc=np.sum)
new_yinsghou_amount=data.reset_index(level=[0,1,2])
old_yinsghou_amount=data2.reset_index(level=[1,2])

over_due=pd.read_sql(over_due_sql,cur)
new_over_due=pd.pivot_table(over_due,values='到期本息',index=['自然月','产品','逾期阶段'],aggfunc=np.sum)
new_over_due=new_over_due.reset_index(level=[0,1,2])
new_over_due.columns=['年月','产品名称','逾期阶段','到期本息']
all_data= pd.merge(new_yinsghou_amount,new_over_due,on=['年月','产品名称','逾期阶段'],how='outer')
print(1)
# all_data=pd.concat([new_yinsghou_amount,new_over_due])
print(2)