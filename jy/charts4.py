import plotly.offline as py
import pandas as pd
from plotly.graph_objs import Scatter, Layout,Figure
py.init_notebook_mode(connected=True)


ccms= pd.read_csv('/home/testuser/ccms_finall.txt',header=0,parse_dates=False)
data=ccms.groupby(['collection_date'])['id_num'].agg('count').reset_index()
data2=data[data['collection_date']>='2018-04-09']
data2['mean']=3767
data3=data2[data2['id_num']<data2['mean']]
data3=data3.loc[:,['collection_date','id_num']]
data3['color']='red'
data3['size']=10

x_axis_template=dict(
    showgrid=False,  #网格
    zeroline=False,  #是否显示基线,即沿着(0,0)画出x轴和y轴
    nticks=10,
    showline=True,
    title='时间',
    mirror='all',
)
y_axis_template=dict(
    showgrid=False,  #网格
    zeroline=False,  #是否显示基线,即沿着(0,0)画出x轴和y轴
    nticks=10,
    showline=True,
    title='催收次数',
    mirror='all',
)
layout=Layout(
    xaxis=x_axis_template,
    yaxis=y_axis_template
)
trace1=Scatter(
    name='每日催收次数',
    x=data2['collection_date'],
    y=data2['id_num']
)
trace2=Scatter(
    name='催收次数分位数',
    x=data2['collection_date'],
    y=data2['mean']
)
trace3=Scatter(
    name='低于分位数每日次数',

    x=data3['collection_date'],
    y=data3['id_num'],
    text=data3['id_num'].tolist(),
    textposition="top center",

    marker=dict(
        color=data3['color'].tolist(),
        size=data3['size'].tolist(),
    ),
     mode='markers+text'
)
data=[trace1, trace2,trace3]
fig=Figure(
    data=data,
    layout=layout
)
py.iplot(fig)