import pandas as pd
from sqlalchemy import create_engine

local=create_engine('mysql+pymysql://root:shitou@10.253.170.0/airflow?charset=utf8')
local=local.connect()


sql="""select dag_id,date_format(execution_date,'%%Y-%%m-%%d %%H:%%m:%%s') date,state from dag_run where DATE(execution_date)=DATE_sub(DATE(now()),INTERVAL 1 DAY )"""

data=pd.read_sql(sql,local)

useless=pd.DataFrame({'dag_id':[None,None],'date':[None,None],'state':['failed','running']})

data=pd.concat([data,useless])

# print(data)
# data2 = data.pivot(index='date', columns='state', values='dag_id').reset_index()
# print(data2)
running=set(data[data['state']=='running'].dag_id.tolist())
success=set(data[data['state']=='success'].dag_id.tolist())
failed=set(data[data['state']=='failed'].dag_id.tolist())

content="""
正在运行的任务:%s
已经完成的任务:%s
失败的任务:%s
"""%(running,success,failed)
print(content)



