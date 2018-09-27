import pandas as pd
from sqlalchemy import create_engine
import pymysql
import datetime
import time
from sms import sendto

while True:
    local=create_engine('mysql+pymysql://root:shitou@localhost/airflow?charset=utf8')
    local=local.connect()
    sql="SELECT dag_id,start_date from task_instance WHERE start_date>='{}' AND state IN ('failed','retery')".format(datetime.datetime.strftime(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S'))
    data=pd.read_sql(sql,local)
    if data.empty:
        pass
    else:
        data['start_date']=data['start_date'].map(lambda x : datetime.datetime.strftime(x,'%Y-%m-%d %H:%M:%S'))
        print(data['dag_id'].tolist())
        sendto(['17721291792', '15822708861', '18301959085'],content='\n时间：%s'%str(data['start_date'].tolist())+'\n任务：%s。\n'%str(data['dag_id'].tolist())+'\n详情：任务失败，请及时修复')
    local.close()
    time.sleep(3600)

#
