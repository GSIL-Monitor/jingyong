import pandas as pd
from sqlalchemy import create_engine


ip='10.253.169.47'
def fq():
    fq=create_engine('mysql+pymysql://otter:otter@123@{}/fqmall_ht_prod?charset=utf8'.format(ip))
    fq=fq.connect()
    return fq


def dw():
    dw=create_engine('mysql+pymysql://otter:otter@123@{}/dw_base?charset=utf8'.format(ip))
    dw=dw.connect()
    return dw

def da():
    da=create_engine('mysql+pymysql://otter:otter@123@{}/da_base?charset=utf8'.format(ip))
    da=da.connect()
    return da

def cc():
    cc=create_engine('mysql+pymysql://otter:otter@123@{}/ccms2?charset=utf8'.format(ip))
    cc=cc.connect()
    return cc

def sc():
    sc=create_engine('mysql+pymysql://telcms:telcmshengyuan@10.139.107.135/telcms?charset=utf8'.format(ip))
    sc=sc.connect()
    return sc

def sx():
    sx=create_engine('mysql+pymysql://otter:otter@123@{}/sx_info?charset=utf8'.format(ip))
    sx=sx.connect()
    return sx

def hy():
    hy=create_engine('mysql+pymysql://otter:otter@123@{}/hy_hawkeye?charset=utf8'.format(ip))
    hy=hy.connect()
    return hy


def conn_close():
    fq().close()
    dw().close()
    cc().close()
    sx().close()
    hy().close()
