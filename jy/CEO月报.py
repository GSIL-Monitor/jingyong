import pandas as pd
import numpy as np
from conn import *
import calendar
import datetime
import time
import calendar

fq=fq()
dw=dw()



today='2018-07-01'#datetime.datetime.today().date()''
year=datetime.datetime.today().year
month=datetime.datetime.today().month

week,cur_month_day=calendar.monthrange(year,month-1)
cur_last_month_day='%s-%s-%s'%(year,month-1,cur_month_day)

day_list=sorted([(x+datetime.timedelta(days=1)).strftime('%Y-%m-%d') for x in pd.date_range(end='%s'%today,periods=6,freq='M')],reverse=True)

day,one,two,three,four,five=day_list

dict_money={'one':one,'two':two,'three':three,'four':four,'five':five}

dict_num={'one_num':one,'two_num':two,'three_num':three,'four_num':four,'five_num':five}


def month_remain(key,month):
    month_reamin_sql="""SELECT
	prin_remain,
	user_type
FROM
	fact_photo_loan
WHERE
	DATE(create_date) = '%s' and date(create_time)>'2018-03-01' and is_end=0"""%month

    month_data=pd.read_sql(month_reamin_sql,dw)
    if month_data.empty:
        data1=pd.DataFrame(data={'user_type':[21,23,33],'%s'%key:[0,0,0]})
    else:
        data1=pd.pivot_table(month_data,index='user_type',values='prin_remain',aggfunc=np.sum).reset_index()
        data1.columns=['user_type','%s'%key]
    return data1


def num_remain(key2,month2):
    month_reamin_sql="""SELECT
	prin_remain,
	user_type
    FROM
        fact_photo_loan
    WHERE
	DATE(create_date) = '%s' and date(create_time)>'2018-03-01' and is_end=0"""%month2

    month_data=pd.read_sql(month_reamin_sql,dw)
    if month_data.empty:
        data2=pd.DataFrame(data={'user_type':[21,23,33],'%s'%key2:[0,0,0]})
    else:
        data2=pd.pivot_table(month_data,index='user_type',values='prin_remain',aggfunc=np.size).reset_index()
        data2.columns=['user_type','%s'%key2]
    return data2

ss=[]
for key,value in dict_money.items():
    dd=month_remain(key,value)
    # print(dd)
    ss.append(dd)

ss2=[]
for key2,value2 in dict_num.items():
    dd2=num_remain(key2,value2)
    # print(dd)
    ss2.append(dd2)

d1=pd.concat(ss,axis=1)
d1.drop(['user_type'],axis=1,inplace=True)
d1['user_type']=[21,23,33]

d2=pd.concat(ss2,axis=1)
d2.drop(['user_type'],axis=1,inplace=True)
d2['user_type']=[21,23,33]
d2['current_month']=one

ds=pd.merge(d1,d2,on='user_type',how='left')

ds.to_sql('ceo_month_report',dw,index=False,if_exists='append')

conn_close()