import pandas as pd
from duoyuan_conn import *
import datetime
dw=ddw()
from sms import sendto as cs

yesterday=datetime.datetime.now().date()-datetime.timedelta(days=1)

sql="""select PAY_BACK_DATE,SUM(amount) money
from pay_back_detail
WHERE PAY_BACK_DATE<='2018-09-17'
GROUP BY PAY_BACK_DATE
"""
data=pd.read_sql_query(sql,dw)
yes_amount=data['money'].values.tolist()[-1]
amount_diff=data['money'].diff().values.tolist()[-1]
yes_amount=round(yes_amount,2)
amount_diff=round(amount_diff,2)

cs(['17721291792','15822708861','18301959085','17718156322','18291969848'],content='昨日回款额：{},与日相比增加:{}'.format(yes_amount,amount_diff))

