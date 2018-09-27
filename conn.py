import pandas as pd
from sqlalchemy import create_engine
import datetime


print(datetime.datetime.now().date())




def fq():
    fq=create_engine('mysql+pymysql://otter:otter@123@10.253.169.47/fqmall_ht_prod?charset=utf8')
    fq=fq.connect()
    return fq


def dw():
    dw=create_engine('mysql+pymysql://otter:otter@123@10.253.169.47/dw_base?charset=utf8')
    dw=dw.connect()
    return dw


def cc():
    cc=create_engine('mysql+pymysql://otter:otter@123@10.253.169.47/ccms2?charset=utf8')
    cc=cc.connect()
    return cc

def sc():
    sc=create_engine('mysql+pymysql://telcms:telcmshengyuan@10.139.107.135/telcms?charset=utf8')
    sc=sc.connect()
    return sc

def sx():
    sx=create_engine('mysql+pymysql://otter:otter@123@10.253.169.47/sx_info?charset=utf8')
    sx=sx.connect()
    return sx


def conn_close():
    fq().close()
    dw().close()
    cc().close()
    sx().close()
