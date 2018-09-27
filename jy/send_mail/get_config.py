import configparser
import os
from conn import *


def get_config(db_name):
    cf = configparser.ConfigParser()
    cf.read("config.ini")

    user = cf.get(db_name, 'user')
    pass_wd = cf.get(db_name, 'passwd')
    port = cf.get(db_name, 'port')
    host = cf.get(db_name, 'host')

    return user, pass_wd, host, port


def get_project(project):
    data=pd.read_sql("select * from mail_set where project='%s'"%project,dw())
    excel_name=data['sheet_name'][0].split(',')
    project_site=data['project_site'][0]
    rec_phone=data['receiver_phone'][0].split(',')
    alarm=data['alarm_phone'][0]
    sender=data['sender'][0]
    receiver=data['receiver'][0].split(',')
    chaosong = data['chaosong'][0].split(',')
    return excel_name,project_site,rec_phone,alarm,sender,receiver,chaosong


print(get_project('test'))