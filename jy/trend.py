import pandas as pd
import matplotlib.pyplot as plt
import matplotlib as mpl
import numpy as np
import pylab as pl
from matplotlib.ticker import FuncFormatter


vip_data = pd.read_excel("E:\\OneDrive\\本机程序\\日报_06-26.xlsx")
vip_data = vip_data.reset_index()
vip_data['日期'] = vip_data['日期'].apply(lambda x: x.strftime('%m-%d'))
vip_data = vip_data.set_index([u'日期']).drop(['产品'], axis=1)
vip_data = vip_data[vip_data['new_old'] == '新客户']
vip_data = vip_data[['系统拒绝数', '人工拒绝数', '总通过数']]




vip_data['通过率'] = vip_data['总通过数'] / (vip_data['系统拒绝数'] + vip_data['人工拒绝数'] + vip_data['总通过数'])
# haimiao_new['通过率'] = haimiao_new['通过率'].apply(lambda x: '%.2f%%' % (x * 100))
# haimiao_new['日期'] = haimiao_new['日期'].apply(lambda x: x[5:])

plt.switch_backend('agg')
mpl.rcParams[u'font.sans-serif'] = ['simhei']
mpl.rcParams['axes.unicode_minus'] = False
# projectname = '/home/cl/daily_report_market'
df = vip_data['通过率'].tolist()
index=list(vip_data.index)
plt.plot(index, df, label='weight changes', linewidth=3, markerfacecolor='blue', markersize=20)
for a, b in zip(index, df):
    plt.text(a, b, '%.2f%%' % (b*100), ha='center', va='bottom', fontsize=11)





def to_percent(temp, position):
    return '%.f%%'%(100*temp)

plt.gca().yaxis.set_major_formatter(FuncFormatter(to_percent))
plt.legend(fontsize="x-small")
plt.xticks(rotation=360)
plt.title(u'{}总通过率'.format('VIP'))
plt.show()
plt.savefig("d:\\{}总通过率.png".format('VIP'))