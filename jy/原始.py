import pandas as pd

from sqlalchemy import create_engine

def jfq():
    fq=create_engine('mysql+pymysql://caoliang:8AEPAe5lw$#0i%p%@10.139.107.144/fqmall_ht_prod?charset=utf8')
    fq=fq.connect()
    return fq


def jdw():
    dw=create_engine('mysql+pymysql://caoliang:8AEPAe5lw$#0i%p%@10.139.107.144/dw_base?charset=utf8')
    dw=dw.connect()
    return dw


def jcc():
    cc=create_engine('mysql+pymysql://caoliang:8AEPAe5lw$#0i%p%@10.139.107.144/ccms2?charset=utf8')
    cc=cc.connect()
    return cc

# def sx():
#     sx=create_engine('mysql+pymysql://otter:otter@123@10.139.107.144/sx_info?charset=utf8')
#     sx=sx.connect()
#     return sx

def conn_close():
    jfq().close()
    jdw().close()
    jcc().close()
    # sx().close()

data=pd.read_sql("select APP_NO,ID_NUM,COLLECTION_DATE,CASE_NOTES,COLLECTION_RESULTS,LATE_STAGE,ACTION_NUM from ccms2.pu_debt_recoard limit 10",jcc())

dic=pd.read_sql("""SELECT DIC_CODE,DIC_NAME from pu_dit_dic""",jcc())

data2=pd.merge(data,dic,left_on='action_num',right_on='DIC_CODE',how='left')

data['app']=[x[0] for x in list(data2['app_no'].fillna(0).astype(str).str.split('.'))]
