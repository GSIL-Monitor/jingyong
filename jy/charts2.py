import pandas as pd
import matplotlib.pyplot as plt

plt.rcParams['font.family'] = ['SimHei']

data=pd.read_csv('d:\\pay_back/sheet3.txt',header=0)

data=data[(data['collection_date']>='2018-04-05') & (data['collection_date']<='2018-07-14')]

fig,ax=plt.subplots(figsize=(20,10))
x=[i for i in range(len(data))]

ax.plot(x,data['count'],marker='o')

plt.ylabel('催收次数')

plt.annotate('起始日期为：2018年4月5日',(30,10000),fontsize=30)

plt.show()