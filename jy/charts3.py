import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import datetime
import pylab as pl
fmt = mdates.DateFormatter('%Y-%m-%d')

ccms= pd.read_csv('d://pay_back/ccms_finall.txt',header=0,parse_dates=False)
data=ccms.groupby(['collection_date'])['id_num'].agg('count').reset_index()
data2=data[data['collection_date']>='2018-04-09']
data2['mean']=3767

# data2.loc[data2['id_num']<data2['mean'],'color']='r'
# data2.loc[data2['id_num']>=data2['mean'],'color']='b'


data3=data2[data2['id_num']<data2['mean']]
data3=data3.loc[:,['collection_date','id_num']]

fig,ax=plt.subplots(figsize=(20,10))

# ax2=ax.twiny()
timeArray = [datetime.datetime.strptime(i,'%Y-%m-%d') for i in data2['collection_date']]

ax.plot(timeArray,data2['id_num'],marker='o')



ax.plot(timeArray,data2['mean'],color='r')
pl.xticks(rotation=0)


timeArray2 = [datetime.datetime.strptime(i,'%Y-%m-%d') for i in data3['collection_date']]
# ax.axes.set_xticks(timeArray2)

ax2=ax.twiny()
ax2=ax.twinx()
ax2.scatter(timeArray2,data3['id_num'],color='r')


for a,b in data3.values:
    plt.text(a, b, b, ha='center', va='bottom', fontsize=8)
# ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))#设置时间标签显示格式

# ax.xaxis_date()
# ax2=ax.twinx()
# ax2=ax.twiny()
plt.show()