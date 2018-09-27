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
