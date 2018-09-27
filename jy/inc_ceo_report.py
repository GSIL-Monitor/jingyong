from conn import *
import datetime
dw=dw()

t1=datetime.datetime.now()
sqls="""
DELETE from   tableau_ceoreport where CREATE_DATE>=date_sub(CURDATE(),interval 1 day);
INSERT into tableau_ceoreport (`CREATE_DATE` ,
  `user_type` ,
  `principal` ,
  `M0prin_remain` ,
  `M1prin_remain` ,
  `M2prin_remain` ,
  `M3prin_remain` ,
  `M4prin_remain` ,
  `M5prin_remain` ,
  `M6prin_remain` ,
  `M7PLUSprin_remain` ,
  `count` ,
  `M0count` ,
  `M1count` ,
  `M2count` ,
  `M3count` ,
  `M4count` ,
  `M5count` ,
  `M6count` ,
  `M7PLUScount`)
select 
date_sub(date(p.create_date),interval 1 day) as '日期',
p.user_type ,
sum(p.prin_remain +p.prin_paid) 放贷金额,

SUM(CASE WHEN IFNULL(p.overdue_level,0) = 0 and  is_end = 0 THEN prin_remain ELSE 0.00 END) AS M0没逾期金额,
SUM(CASE WHEN IFNULL(p.overdue_level,0) = 1 THEN prin_remain ELSE 0.00 END) AS M1逾期一个月内金额,
SUM(CASE WHEN IFNULL(p.overdue_level,0) = 2 THEN prin_remain ELSE 0.00 END) AS M2逾期一到二个月金额,
SUM(CASE WHEN IFNULL(p.overdue_level,0) = 3 THEN prin_remain ELSE 0.00 END) AS M3逾期二到三个月金额,
SUM(CASE WHEN IFNULL(p.overdue_level,0) = 4 THEN prin_remain ELSE 0.00 END) AS M4逾期三到四个月金额,
SUM(CASE WHEN IFNULL(p.overdue_level,0) = 5 THEN prin_remain ELSE 0.00 END) AS M5逾期四到五个月金额,
SUM(CASE WHEN IFNULL(p.overdue_level,0) = 6 THEN prin_remain ELSE 0.00 END) AS M6逾期五到六个月金额,
SUM(CASE WHEN IFNULL(p.overdue_level,0) >= 7 THEN prin_remain ELSE 0.00 END) AS 'M7+逾期超过六个以上月金额',
count(*) 放贷笔数,
SUM(CASE WHEN IFNULL(p.overdue_level,0) = 0 and is_end = 0 THEN 1 ELSE 0 END) AS M0没逾期笔数,
SUM(CASE WHEN IFNULL(p.overdue_level,0) = 1 THEN 1 ELSE 0 END) AS M1逾期一个月内笔数,
SUM(CASE WHEN IFNULL(p.overdue_level,0) = 2 THEN 1 ELSE 0 END) AS M2逾期一到二个月笔数,
SUM(CASE WHEN IFNULL(p.overdue_level,0) = 3 THEN 1 ELSE 0 END) AS M3逾期二到三个月笔数,
SUM(CASE WHEN IFNULL(p.overdue_level,0) = 4 THEN 1 ELSE 0 END) AS M4逾期三到四个月笔数,
SUM(CASE WHEN IFNULL(p.overdue_level,0) = 5 THEN 1 ELSE 0 END) AS M5逾期四到五个月笔数,
SUM(CASE WHEN IFNULL(p.overdue_level,0) = 6 THEN 1 ELSE 0 END) AS M6逾期五到六个月笔数,
SUM(CASE WHEN IFNULL(p.overdue_level,0) >= 7 THEN 1 ELSE 0 END) AS 'M7+逾期超过六个以上月笔数'

from fact_photo_loan p
where p.create_time > '2018-03-01 00:00:00'
and p.create_date>CURDATE()
-- and left(p.create_date,10) >= date_sub(left(now(),10), interval 31 day)
GROUP BY left(p.create_date,10),p.user_type;

#更新应收
update tableau_ceoreport inner join 
(select lrp.DUE_DATE,f.user_type,sum(lrp.PRINCIPAL) principal_ys,sum(lrp.TOTAL_AMT-lrp.PENALTY_INT-lrp.LPC) total_eptlpc
from fact_photo_loan f inner join fqmall_ht_prod.loan_repay_plan lrp on lrp.LOAN_ID=f.loan_id
where f.create_date>CURDATE()
and f.create_time>'2018-03-01'
and lrp.CURRENT_TERM<>0
and lrp.DUE_DATE=date_sub(CURDATE(),INTERVAL 1 day)
group by lrp.DUE_DATE,f.user_type) a on tableau_ceoreport.user_type=a.user_type and tableau_ceoreport.CREATE_DATE=a.due_date
set tableau_ceoreport.principal_ys=a.principal_ys , 
tableau_ceoreport.total_eptlpc=a.total_eptlpc;



update tableau_ceoreport inner join 
(#回款状况
select 
date(B.create_date) create_date,
b.user_type,
sum(a.mth_paid-b.mth_paid) + sum(a.info_paid- b.info_paid) + sum(a.fee1_paid- b.fee1_paid) + sum(a.fee2_paid- b.fee2_paid) 
+ sum(a.fee3_paid-b.fee3_paid) + sum(a.fee4_paid-b.fee4_paid) HSFY, 
sum(a.lpc_paid - b.lpc_paid) + sum(a.penalty_paid - b.penalty_paid)  HSWYJ, 
sum(a.prin_paid-b.prin_paid) + sum(a.int_paid-b.int_paid)  HSBX
from fact_photo_loan as b inner JOIN fact_photo_loan as a ON  -- datediff(date(a.create_date),date(b.create_date))=1 and
a.loan_id = b.loan_id 
where b.create_time >= '2018-03-01 00:00:00'
and B.create_date BETWEEN date_format(date_sub(CURDATE(),INTERVAL 1 day),'%Y-%m-%d %H:%i:%S') and date_format(CURDATE(),'%Y-%m-%d %H:%i:%S')
and a.create_date BETWEEN date_format(CURDATE(),'%Y-%m-%d %H:%i:%S') and date_format(CURDATE(),'%Y-%m-%d 23:59:59')
group by create_date,user_type) a on tableau_ceoreport.user_type=a.user_type and tableau_ceoreport.CREATE_DATE=a.create_date
set tableau_ceoreport.HSFY=a.HSFY , 
tableau_ceoreport.HSWYJ=a.HSWYJ,
tableau_ceoreport.HSBX=a.HSBX;


update tableau_ceoreport inner join 
(
#客户表现
SELECT 
date(f1.create_date) creat_date,
f1.user_type,
sum(CASE when f1.overdue_level=0 and f1.due_date=date(f1.create_date) then f1.prin_cur_topay else 0.00 end) lastday_M0prin_ys,
sum(CASE when f1.overdue_level=0 and f1.due_date=date(f1.create_date) then f2.prin_cur_paid else 0.00 end)  lastday_M0prin_paid,
sum(CASE when f1.overdue_level=1  then 1 else 0 end) lastday_M1count,
sum(CASE when f1.overdue_level=1 and f2.overdue_level=0  then 1 else 0 end) lastday_M0count,
sum(CASE when f1.overdue_level=2  then 1 else 0 end) lastday_M2count,
sum(CASE when f1.overdue_level=2 and f2.overdue_level in (1,0)  then 1 else 0 end) `lastday_M0OR1count`
FROM `fact_photo_loan` f1 inner join `fact_photo_loan` f2 on f1.application_no=f2.application_no -- and datediff(f2.create_date,f1.create_date)=1 -- and datediff(date(f2.create_date),date(f1.create_date))=1

where 
-- f1.create_date  BETWEEN date_format(date_sub(CURDATE(),INTERVAL 31 day),'%Y-%m-%d %H:%i:%S') and date_format(CURDATE(),'%Y-%m-%d 23:59:59')
-- and 
f1.create_time>='2018-03-01 00:00:00' and f2.create_time>='2018-03-01 00:00:00'
and f1.create_date BETWEEN date_format(date_sub(CURDATE(),INTERVAL 1 day),'%Y-%m-%d %H:%i:%S') and date_format(CURDATE(),'%Y-%m-%d %H:%i:%S')
and f2.create_date BETWEEN date_format(CURDATE(),'%Y-%m-%d %H:%i:%S') and date_format(CURDATE(),'%Y-%m-%d 23:59:59')
-- and 
group by creat_date,user_type
) a on tableau_ceoreport.user_type=a.user_type and tableau_ceoreport.CREATE_DATE=a.creat_date
set tableau_ceoreport.lastday_M0prin_ys=a.lastday_M0prin_ys , 
tableau_ceoreport.lastday_M0prin_paid=a.lastday_M0prin_paid,
tableau_ceoreport.lastday_M1count=a.lastday_M1count,
tableau_ceoreport.lastday_M0count=a.lastday_M0count,
tableau_ceoreport.lastday_M2count=a.lastday_M2count,
tableau_ceoreport.lastday_M0OR1count=a.lastday_M0OR1count
;

update tableau_ceoreport inner join 
(
select date(a.create_time) creat_date,
sum(a.prin_remain +a.prin_paid) as totalprincipal_add, 
COUNT(*) as count_add,
a.user_type
from fact_photo_loan a
where 
 a.create_time>='2018-05-24 00:00:00'
-- and a.create_time < date_format(CURDATE(),'%Y-%m-%d %H:%i:%S')
and a.create_date  BETWEEN date_format(CURDATE(),'%Y-%m-%d %H:%i:%S') and date_format(CURDATE(),'%Y-%m-%d 23:59:59')
and a.create_time  BETWEEN date_format(date_sub(CURDATE(),interval 1 day),'%Y-%m-%d %H:%i:%S') and 
date_format(date_sub(CURDATE(),interval 1 day),'%Y-%m-%d 23:59:59')
-- and a.create_time>'2018-03-01 00:00:00'
GROUP BY date(a.create_time),a.user_type) a on tableau_ceoreport.user_type=a.user_type and tableau_ceoreport.CREATE_DATE=a.creat_date
set tableau_ceoreport.totalprincipal_add=a.totalprincipal_add , 
tableau_ceoreport.count_add=a.count_add
"""

for i,j in enumerate(sqls.split(';')):
    dw.execute(j)
    print('step %s done'%i)
t2=datetime.datetime.now()
print(t2-t1)
