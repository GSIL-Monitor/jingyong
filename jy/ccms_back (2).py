import pandas as pd

ccms= pd.read_csv('/home/testuser/ccms_finall.txt',header=0)
back=pd.read_csv('/home/testuser/back.txt',header=0)
week=pd.read_csv('/home/testuser/caoliang/weekday.txt',header=0)



pay_back=back.groupby(['loan_id','chongzhi_date'])['acct_prin'].agg(['sum']).reset_index()
pay_back.columns=['app_no','chongzhi_date','sum']


data3=ccms.groupby(['LOAN_ID','collection_date'])['case_notes'].count().reset_index()

data4=pd.merge(data3,week,left_on='collection_date',right_on='day',how='left').drop(['day'],axis=1)
data4['is_work'].fillna('yes',inplace=True)
data4['LOAN_ID']=data4['LOAN_ID'].astype(int)
data4.columns=['loan_id','collection_date','case_notes','is_work']
data5=pd.merge(pay_back,data4,on='loan_id',how='left')
data6=data5[~data5['collection_date'].isnull()]
data7=data6[data6['chongzhi_date']>data6['collection_date']]
data8=data7.pivot_table(index=['loan_id','chongzhi_date'],columns='is_work',values=['case_notes'],aggfunc=sum).reset_index()
data8=data8.sort_values(by=['app_no','chongzhi_date'],ascending=False)
data8.fillna(0,inplace=True)
data8.reset_index(inplace=True)
data8.drop(['index'],axis=1,inplace=True)
diff_no=[]
diff_yes=[]
for i in range(len(data8.index)-1):
    t1=data8.loc[i,'app_no']
    t2=data8.loc[i+1,'app_no']
    print(t1,t2)
    if t1==t2:
        diffno=data8.loc[i,'no']-data8.loc[i+1,'no']
        diffyes=data8.loc[i,'yes']-data8.loc[i+1,'yes']
        diff_no.append(diffno)
        diff_yes.append(diffyes)
    else:
        diff_no.append(data8.loc[i,'no'])
        diff_yes.append(data8.loc[i,'yes'])
diff_no.append(data8.loc[len(data8.index),'no'])
diff_yes.append(data8.loc[len(data8.index),'yes'])
data8.drop(['no','yes'],axis=1,inplace=True)
fin1=pd.merge(pay_back,data8,on=['app_no','chongzhi_date'],how='inner')
fin1.to_csv('/home/testuser/caoliang/fin1.csv',index=False)

#----sheet2---
fin2=fin1.groupby('chongzhi_date')['week','work','sum'].agg('sum').reset_index()
fin2['chongzhi_date']=fin2['chongzhi_date'].astype('str')
fin2['day']=[x[0] for x in fin2['chongzhi_date'].str.split(' ')]
fin3=pd.merge(fin2,week,left_on='day',right_on='day',how='left').drop(['day'],axis=1)
fin3['is_work'].fillna('yes',inplace=True)
fin3.to_csv('/home/testuser/caoliang/fin3.csv',index=False)
