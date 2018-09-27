import pandas as pd
from sqlalchemy import create_engine

def fq():
    fq=create_engine('mysql+pymysql://dw_dev:hydw@2018@10.253.169.47/fqmall_ht_prod?charset=utf8')
    fq=fq.connect()
    return fq


def dw():
    dw=create_engine('mysql+pymysql://dw_dev:hydw@2018@10.253.169.47/dw_base?charset=utf8')
    dw=dw.connect()
    return dw


def cc():
    cc=create_engine('mysql+pymysql://dw_dev:hydw@2018@10.253.169.47/ccms2?charset=utf8')
    cc=cc.connect()
    return cc

def close():
    fq().close()
    dw().close()
    cc().close()
