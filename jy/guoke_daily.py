import pandas as pd
from sqlalchemy import create_engine
import re
import yagmail
import glob
import os
from datetime import datetime
import time
import zipfile
print(1111)


os.system('rm -f -r /home/cl/daily_report_guoke')
# time.sleep(100)
con=create_engine('mysql+pymysql://dongdong:hengyuan.2018@121.43.73.95:2800/fqmall_ht_prod?charset=utf8')
conn=con.connect()
sender='caoliang'
cc=['leo.zhao@hengyuan-finance.com','emily.zhang@hengyuan-finance.com','charles.lin@hengyuan-finance.com',
    'colin.wang@hengyuan-finance.com','yuhaifeng@dpandora.cn','dongdong.zhai@hengyuan-finance.com']
receiver=['tianyong.hu@hengyuan-finance.com','caoliang@dpandora.cn']#
# receiver=['caoliang@dpandora.cn']
# cc=[]
projectname='/home/cl/daily_report_guoke'
os.mkdir('/home/cl/daily_report_guoke')

hx="""SELECT CURDATE() 投保日期,m.applicationno 流水号,m.name 投保人姓名,'00' 与被保险人关系,m.`name` 被保险人姓名,d.gender 性别,DATE(mid(m.identityno,7,8)) 出生日期,'0' 证件类型,m.identityno 身份证号,'1' `保险期间（月）`, l.DUE_DATE `投保起期（还款日）`,l.add_fee4_paid `保费（当月保费）`
from mis_loan_detail m inner join d_input_app d on m.applicationno=d.applicationNo
inner join loan_repay_plan l on l.LOAN_ID=m.loan_id
where  l.add_fee4=l.add_fee4_paid
and m.available_date>'2018-04-10'
and l.add_fee4_paid>0
and l.CURRENT_TERM<>0
and l.DUE_DATE<CURDATE();"""
hx_data= pd.read_sql_query(hx,conn)
hx_data['流水号']=hx_data['流水号'].astype(str)
hx_data.to_excel('%s/%s.xlsx'%(projectname,'Attachment-2-HuaXia-%s'%datetime.now().date()),index=False)


zct="""SELECT 
m.available_date `投保日期（借款起期）` ,
m.applicationno 订单号,
'1100010710' 合作机构,
'大同果壳金融信息服务有限公司' 团体单位名称,
'山西省大同市开发区樊庄花园7号楼3单元5号' 单位地址,
'雷萍'  单位联系人,
'021-24253333' 单位联系电话,
'insurance@11bee.com' 投保人邮箱,
'91140200MA0JU84PXR' 统一社会信用代码,
m.name 被保人人姓名,
dia.gender 被保人性别,
date(mid(m.identityno,7,8))被保人出生日期 ,
'身份证' 被保人证件类型,
m.identityno 被保人证件号码,
'021-24253333' 被保人电话,
'一类'  被保人职业,
date(m.available_date) `保险合同生效日（借款起期）`,
m.totalterm 保险期间,#4.11修改为期数#
'M' 保险期间单位,
m.available_date  `保险开始时间（借款起期）`,
m.end_date   `保险结束时间（借款止期）`,
'1' 保费 ,
A.premium_PAID 份数,
A.premium_amount 保额,
(case when m.totalterm=3 then 'B62002'  when m.totalterm=6 then 'C62002' else  '' end) 保障计划
FROM 
additional_fee_detail a  INNER JOIN mis_loan_detail m ON a.hy_application_no=m.applicationno
                         left join d_input_app dia on dia.applicationNo=a.hy_application_no
WHERE a.company_id in ('5','1')
and ifnull(iscancel,'')<>'Y'
#and left(m.available_date,7)='2018-05'
and left(m.available_date,7)>=left(date_sub(CURDATE(), INTERVAL 1 month),7);"""
zct_data= pd.read_sql_query(zct,conn)
zct_data['订单号']=zct_data['订单号'].astype(str)
zct_data.to_excel('%s/%s.xlsx'%(projectname,'Attachment-1-ZhongChengTai-%s'%datetime.now().date()),index=False)

yg="""SELECT 
m.applicationno 订单号,'大同果壳金融信息服务有限公司' 投保人名称,
m.name 姓名,
m.identityno 身份证号码,
'021-33310260' 被保人电话,
m.totalprincipal 合同本金,
m.available_date 借款起期,
m.end_date  借款止期,
a.premium_paid 保险费金额,
m.totalterm 借款期数
FROM 
additional_fee_detail a  INNER JOIN mis_loan_detail m ON a.hy_application_no=m.applicationno
WHERE a.company_id in ('6','2')
and ifnull(iscancel,'')<>'Y'
and left(m.available_date,7)>=left(date_sub(CURDATE(), INTERVAL 1 month),7)"""
yg_data= pd.read_sql_query(yg,conn)
yg_data['订单号']=yg_data['订单号'].astype(str)
yg_data.to_excel('%s/%s.xlsx'%(projectname,'Attachment-1-YangGuang-%s'%datetime.now().date()),index=False)

names = "','".join(['%s' for _ in range(len(yg_data['订单号']))])% tuple(yg_data['订单号'].tolist())
writer=pd.ExcelWriter('%s/%s.xlsx'%(projectname,'Attachment-1-Compare'))


chuxun1="""select T.APP_NO '流水号',sum(T.amount) '扣款金额' from xf_collection t where t.COLLECTION_NO like '%s%%' and T.COLLECTION_STATUS='WHSU' and t.RESP_MSG not like '%%%s%%'  and t.APP_NO in ('%s') group by t.APP_NO"""%("bxf%","%已%",names)
chuxun1_data= pd.read_sql(chuxun1,conn)
chuxun1_data.to_excel(writer,index=False,sheet_name='XF')

chuxun2="select T.APP_NO '流水号',sum(T.amount) '扣款金额' from BF_collection t where t.COLLECTION_NO like '%s%%' and T.COLLECTION_STATUS='WHSU' and t.RESP_MSG not like '%%%s%%' and APP_NO in ('%s') group by t.APP_NO;"% ('RISK%','%已%',names)
chuxun2_data= pd.read_sql_query(chuxun2,conn)
chuxun2_data.to_excel(writer,index=False,sheet_name='BF')
writer.save()
conn.close()


content="""
Dear   Tianyong:
    Attachment-1-YangGuang:  附件1阳光投保
    Attachment-1-ZhongChengTai：  附件1众诚泰保费
    Attachment-2-HuaXia:  附件2华夏保费
    Attachment-1-Compare： 附件1对照表
——————————————————————————————————————————————————————————————————
    Attachment-1为本月度截止基准日的新业务投保数据,根据之前沟通，本次所提供投保数据会对照表使用。
    Attachment-2为华夏保费数据，到期已还足保险费，请知悉。
————————————
曹梁
大数据研发部"""
# attglob.glob(r'%s/*.xlsx'%projectname)
# z=zipfile.ZipFile('%s/%s.zip'%(projectname,datetime.now()),'w')
# for i in glob.glob('%s/*.xlsx'%projectname):
#     z.write(i)
# z.close()
os.system('zip -rP %s %s/%s.zip  /home/cl/daily_report_guoke'%(datetime.now().date(),projectname,datetime.now().date()))



yag = yagmail.SMTP(user="caoliang@dpandora.cn", password="Shitou12121", host="smtp.exmail.qq.com", port="465")
yag.send(receiver,sender,contents=content,attachments='%s/%s.zip'%(projectname,datetime.now().date()),cc=cc)
yag.close()




print('send success')

