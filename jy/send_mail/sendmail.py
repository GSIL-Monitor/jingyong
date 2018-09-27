import pandas as pd
import yagmail
import os
from datetime import datetime
from get_config import get_config
from create_passwd import create_passwd
from sms import sendto
from conn import *
from one_excel import one_sheet
from get_config import get_project
# from Jingxiang import *


# #生成单个sheet文件
# def one_sheet2(sql,conn):
#     sql_data= pd.read_sql_query(sql,conn)
#     sql_data.to_excel('%s/%s.xlsx'%(project_site,'生成文件名及日期-%s'%datetime.now().date()),index=False)
#
#
# #一个excel中生成多个sheet
# def two_sheet(sheet_1_sql,sheet_2_sql,conn):
#     writer=pd.ExcelWriter('%s/%s.xlsx'%(project_site,'文件名')) #
#
#     sheet_1_data= pd.read_sql(sheet_1_sql,conn)
#     sheet_1_data.to_excel(writer,index=False,sheet_name='sheet1的名字')
#
#     sheet_2_data= pd.read_sql_query(sheet_2_sql,conn)
#     sheet_2_data.to_excel(writer,index=False,sheet_name='sheet2的名字')
#     writer.save()


# def mail(content,project_site,project,sec_num,cc):
#
#     #对生成的文件进行加密,并作为附件发送
#     if len(sec_num)>0:
#         os.system("zip -rj -P %s %s/%s.zip %s"%(sec_num,project_site,project,project_site))
#     else:
#         os.system("zip -rj  %s/%s.zip %s" % (project_site, project, project_site))
#     #发送邮件
#     yag = yagmail.SMTP(user="%s"%mailset[0], password="%s"%mailset[1], host="%s"%mailset[2], port="%s"%mailset[3])
#     yag.send(receiver,sender,cc=cc,contents=content,attachments='%s/%s.zip'%(project_site,project))
#     yag.close()


def setting(user,sql_loc,conn,project,content_loc,mima=True):
    # try:
    mailset = get_config('%s'%user)  # 获取邮件配置文件\

    if mima:
        sec_num = create_passwd()  # 生成随机压缩密码
    else:
        sec_num = []

    excel_name, project_site, rec_phone, alarm, sender, receiver, chaosong = get_project(project)  # 生成的文件存放位置

    if os.path.exists(project_site):
        os.system('rm -f -r %s' % project_site)  # 删除昨日生成的数据
        os.mkdir(project_site)  # 新建今日的数据存放位置
    else:
        os.mkdir(project_site)

    with open('/home/mail/mail_sql/%s'%sql_loc,'r') as f:
        sql=f.readlines()
    sql=''.join(sql)
    print(sql)
    one_sheet(sql, conn, project)

    receiver.append(mailset[0])  # .append(i for i in receiver)  # 收件人
    cc = chaosong  # 抄送人
    with open('/home/mail/mail_/%s'%content_loc,'r') as f:
        cont=f.readlines()
    content=''.join(cont)

    if len(sec_num) > 0:
        os.system("zip -rj -P %s %s/%s.zip %s" % (sec_num, project_site, project, project_site))
    else:
        os.system("zip -rj  %s/%s.zip %s" % (project_site, project, project_site))
        # 发送邮件
    yag = yagmail.SMTP(user="%s" % mailset[0], password="%s" % mailset[1], host="%s" % mailset[2],
                       port="%s" % mailset[3])
    yag.send(receiver, sender, cc=cc, contents=content, attachments='%s/%s.zip' % (project_site, project))
    yag.close()
    # print(receiver, rec_phone, str(sec_num))
    if sec_num == []:
        print('无密码')
    else:
        sendto(rec_phone, '附件%s邮件的解压密码是：%s' % (project, str(sec_num)))
    # except:
    #     print(111111111111111111)
    #     sendto(alarm, '%s邮件发送失败' % project)
    conn_close()

if __name__=="__main__":
    setting('caoliang','test',dw(),'test2','test')