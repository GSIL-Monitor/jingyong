import pandas as pd
import matplotlib.pyplot as plt


data=pd.read_csv('d:\\pay_back/fin3.csv',header=0)
data.loc[data['is_work']=='no','color']='b'
data.loc[data['is_work']=='yes','color']='r'
x1=data['chongzhi_date'].tolist()[:10]
y1=data['week'].tolist()[:10]
color=data['color'].tolist()[:10]

fig,ax1 = plt.subplots()
ax2=ax1.twinx()
ax1.plot(x1,y1,marker='x')
ax2.scatter(x1,y1,s=100,color=color)
plt.show()