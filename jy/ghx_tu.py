import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import dates as mdate

data=pd.read_excel('d:\\0918.xlsx',header=0,index_col='日期')
data.plot()
# fig1 = plt.figure(figsize=(10, 5))
#
# ax1 = fig1.add_subplot(1, 1, 1)
# pandas_old_data = data.set_index("日期")
# ax1.xaxis.set_major_formatter(mdate.DateFormatter('%m-%d'))
# ac1 = [x for x in pandas_old_data.index]
# plt.xticks(ac1, rotation=90)
# plt.plot(pandas_old_data['海尔'], 'o-', c='r', label='海尔')
# plt.plot(pandas_old_data['华融'], 'o-', c='b', label='华融')
# plt.plot(pandas_old_data['北银'], 'o-', c='c', label='北银')
# plt.plot(pandas_old_data['自有'], 'o-', c='k', label='自有')
# plt.plot(pandas_old_data['大丰收'], 'o-', c='g', label='大丰收')
# plt.plot(pandas_old_data['笑脸'], 'o-', c='brown', label='笑脸')
# plt.plot(pandas_old_data['小诺'], 'o-', c='m', label='小诺')
# plt.plot(pandas_old_data['口袋'], 'o-', c='y', label='口袋')
# plt.plot(pandas_old_data['广群'], 'o-', c='orange', label='广群')
# plt.plot(pandas_old_data['晋商'], 'o-', c='lime', label='晋商')
# plt.plot(pandas_old_data['总计'], 'o-', c='gold', label='总计')
# plt.legend(loc='upper right')
# plt.show()
# plt.title(u'老资产运营统计折线图')


