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
    data=pd.read_sql('select * from mail_set where project=%s'%project)

    excel_name=data['sheet_name'][0]
    project_site=data['project_site'][0]
    rec_phone=data['receiver_phone'].str.split(',')
    alarm=data['alarm_phone'][0]
    return excel_name,project_site,rec_phone,alarm

