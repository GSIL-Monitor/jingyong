
import os
import pymysql
os.chdir('e:\\hy')
conn = pymysql.connect(
    host="121.43.73.95",
    user="huitongfq_prod",
    passwd="hy-123456.com",
    port=2800,
    charset="utf8",
    db='mysql',
    use_unicode=False
)
cur=conn.cursor()
files=os.listdir('e:\\hy')#report\\report\\aliyun_fin_report\\datadump\\chsi
# s1=[]
# s2=[]
# s3=[]
sql="""insert into jsontest (id_card,reportid,json) VALUES (%s,%s,%s)"""
for file in files:
    print(file)
    line=file.split('.')[0].split('_')
    s1=line[0]
    s2=line[1]
    with open(file,'r',encoding='utf8')as f:
        s3=f.readline()
    cur.execute(sql,(s1,s2,s3))
    print(s1,s2,s3)
conn.commit()
conn.close()
