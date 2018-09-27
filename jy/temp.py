# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import numpy as np
'''
np.repeat(1,10) #把1重复10次
np.repeat(np.arange(1,3),3) # 将其中每个元素扩展3次 生成array([1,1,1,2,2,2])
np.concatenate(...,axis=1) / np.r_ / np.hstack  # 按行合并
np.concatenate(...,axis-0) / np.c_ / np.vstac  #按列合并

np.intersect1d(a,b) #a,b中共同存在的元素
np.setdiff1d #删除a中存在b的元素

np.where  返回的是对应条件元素的位置  np.where(条件,值1,值2)
np.take  通过索引找元素


将python中单个元素的运算换成对整个数组的运算

np.frompyfunc(函数，输入元素个数，输出元素个数) 
np.vectorize(函数)


生成5-10之间的5*3的数组
np.random.uniform(5,10,size=(5,3))


设置显示精度
np.set_printoptions(precision=3)


将数值归一化到0~1之间
（数值-最小值）/最大值-最小值

ss=np.arange(10).reshape(5,2)

计算两列之间的相似性
np.corrcoef[A列，B列]
from scipy.stats.stats import pearsonr


排序
np.sort(axis=0) 列排序
np.argsort(axis=1) #行排序，返回索引
'''