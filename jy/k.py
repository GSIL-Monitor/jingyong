# from numpy import *
import numpy as np
import matplotlib.pyplot as plt

def loaddata(filename):
    datamat=[]
    fr=open(filename)
    for line in fr.readlines():
        curline=line.strip().split('\t')
        fltline=map(float,curline)
        datamat.append(fltline)
    return datamat

def distclud(vecA,vecB):
    return np.sqrt(np.sum(np.power(vecA-vecB,2)))

def randcent(dataset,k):
    n=np.shape(dataset)
    n=n[1]
    centroids=np.mat(np.zeros((k,n)))  #返回K*n的全0矩阵
    for j in range(n):
        minj=min(dataset[:,j])
        rangej=float(max(dataset[:,j])-minj)
        ran=np.random.rand(k,1)  #返回k行，1列的 0-1之间的随机数
        centroids[:,j]=minj+rangej*np.random.rand(k,1)
    return centroids

aa=np.array([[1,2],[2,3],[3,4],[4,5]])

# print(randcent(aa,2))

def kmeans(dataset,k,distmeans=distclud,createcent=randcent):
    m=np.shape(dataset)[0]
    clusterassment= np.mat(np.zeros((m,2))) #用来存贮每个元素的列表和误差平方和
    centriods=createcent(dataset,k)
    clusterchange=True
    while clusterchange:
        clusterchange=False
        for i in range(m):
            mindist=np.inf
            minindex=-1
            for j in range(k):
                distji=distmeans(centriods[j,:],dataset[i,:])
                if distji <mindist:
                    mindist=distji
                    minindex=j
            if clusterassment[i,0] !=minindex:
                clusterchange=True
            clusterassment[i,:]=minindex,mindist**2#:SSE 误差平方和
        # print(centriods)
        for cent in range(k):
            yy=clusterassment[:,0].A==cent #取出每个类别的元素
            xx=np.nonzero(yy) #返回一个tuple,其中tuple[0]是非0元素的下标。
            ptsinclus = dataset[np.nonzero(clusterassment[:,0].A==cent)[0]]  #分别取出各分类的数组
            centriods[cent,:]=np.mean(ptsinclus,axis=0)
    return centriods,clusterassment


centriods,clusterassment=kmeans(aa,2)
centriods=centriods.A
print(centriods)
print('-------------------------------------------')
print(clusterassment)
x=centriods[:,0]
y=centriods[:,1]
plt.scatter(x=aa[:,0],y=aa[:,1])
plt.scatter(x,y,marker='x')
plt.show()
