import pandas as pd
import datetime
from duoyuan_conn import *
from send_mail import sendmail

cs=dcs()
now_day=datetime.datetime.today().date()
last_friday=now_day-datetime.timedelta(days=3)
last_monday=now_day-datetime.timedelta(days=7)
print(now_day,last_friday,last_monday)

sql1="""select '百融'as tag ,count(1) num from HUNDRED_MELT_INFO h where h.updateTime BETWEEN '%s' and '%s'
UNION 
select '前海征信'as tag ,count(1) num from t_qhzx_risk_info t where t.createDate BETWEEN '%s' and '%s'
UNION 
select '同盾'as tag ,count(1) num from t_td_risk t where t.create_time BETWEEN '%s' and '%s'
UNION 
select 'face++'as tag ,count(1) num from fqmall_ht_prod.face_original_detail f where f.create_date BETWEEN '%s' and '%s';"""%(last_monday,last_friday,last_monday,last_friday,last_monday,last_friday,last_monday,last_friday)
print(sql1)
bairong=pd.read_sql(sql1,cs)
sendmail.setting('zhangzhiqiang',cs,'luhuadan',sql_loc=False,content_loc='征信机构调用量',mima=False)