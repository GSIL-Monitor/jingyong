from conn import *

sql1='''update tableau_ceoreport inner join 
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
and B.create_date BETWEEN date_format(date_sub(CURDATE(),INTERVAL 1 day),'%%Y-%%m-%%d %%H:%%i:%%S') and date_format(CURDATE(),'%%Y-%%m-%%d %%H:%%i:%%S')
and a.create_date BETWEEN date_format(CURDATE(),'%%Y-%%m-%%d %%H:%%i:%%S') and date_format(CURDATE(),'%%Y-%%m-%%d 23:59:59')
group by create_date,user_type) a on tableau_ceoreport.user_type=a.user_type and tableau_ceoreport.CREATE_DATE=a.create_date
set tableau_ceoreport.HSFY=a.HSFY , 
tableau_ceoreport.HSWYJ=a.HSWYJ,
tableau_ceoreport.HSBX=a.HSBX;'''

dw=dw()
dw.execute(sql1)