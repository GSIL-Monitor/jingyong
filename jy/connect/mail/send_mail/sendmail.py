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


#生成单个sheet文件
def one_sheet2(sql,conn):
    sql_data= pd.read_sql_query(sql,conn)
    sql_data.to_excel('%s/%s.xlsx'%(project_site,'生成文件名及日期-%s'%datetime.now().date()),index=False)


#一个excel中生成多个sheet
def two_sheet(sheet_1_sql,sheet_2_sql,conn):
    writer=pd.ExcelWriter('%s/%s.xlsx'%(project_site,'文件名')) #

    sheet_1_data= pd.read_sql(sheet_1_sql,conn)
    sheet_1_data.to_excel(writer,index=False,sheet_name='sheet1的名字')

    sheet_2_data= pd.read_sql_query(sheet_2_sql,conn)
    sheet_2_data.to_excel(writer,index=False,sheet_name='sheet2的名字')
    writer.save()


def mail(content,sec_num,name):

    #对生成的文件进行加密,并作为附件发送
    if len(sec_num)>0:
        os.system("zip -rj -P %s %s/%s.zip %s"%(sec_num,project_site,excel_name,project_site))
    else:
        os.system("zip -rj  %s/%s.zip %s" % (sec_num, project_site, excel_name, project_site))
    #发送邮件
    yag = yagmail.SMTP(user="%s"%mailset[0], password="%s"%mailset[1], host="%s"%mailset[2], port="%s"%mailset[3])
    yag.send(receiver,sender,contents=content,attachments='%s/%s.zip'%(project_site,datetime.now().date()),cc=cc)
    yag.close()




if __name__=="__main__":

    mailset = get_config('caoliang') #获取邮件配置文件
    sec_num=create_passwd() #生成随机压缩密码
    excel_name,project_site,rec_phone,alarm = get_project('test')  # 生成的文件存放位置


    if os.path.exists(project_site):
        os.system('rm -f -r %s' % project_site)  # 删除昨日生成的数据
        os.mkdir(project_site)           # 新建今日的数据存放位置
    else:
        os.mkdir(project_site)

    sql="select * from loan_loan limit 10"

    one_sheet(sql,fq(),project_site)


    sender = 'caoliang'
    receiver = [mailset[0]]  # 收件人
    cc = [11]  # 抄送人
    try:
        mail('test   test',sec_num,name)
        sendto(rec_phone,str(sec_num))
    except:
        sendto(alarm,'%s邮件发送失败'%excel_name)

    conn_close()