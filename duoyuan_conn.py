from sqlalchemy import create_engine

ip='10.253.5.147'
def dfq():
    fq=create_engine('mysql+pymysql://caoliang:8AEPAe5lw$#0i%p%@{}/fqmall_ht_prod?charset=utf8'.format(ip))
    fq=fq.connect()
    return fq


def ddw():
    dw=create_engine('mysql+pymysql://caoliang:8AEPAe5lw$#0i%p%@{}/dw_base?charset=utf8'.format(ip))
    dw=dw.connect()
    return dw



def dcc():
    cc=create_engine('mysql+pymysql://caoliang:8AEPAe5lw$#0i%p%@{}/ccms2?charset=utf8'.format(ip))
    cc=cc.connect()
    return cc

def dsc():
    sc=create_engine('mysql+pymysql://caoliang:8AEPAe5lw$#0i%p%@{}/telcms?charset=utf8'.format(ip))
    sc=sc.connect()
    return sc

def dninefive():
    nf=create_engine('mysql+pymysql://caoliang:8AEPAe5lw$#0i%p%@{}/95_backup?charset=utf8'.format(ip))
    nf=nf.connect()
    return nf

def dhy():
    hy=create_engine('mysql+pymysql://caoliang:8AEPAe5lw$#0i%p%@{}/hy_hawkeye?charset=utf8'.format(ip))
    hy=hy.connect()
    return hy

def dcs():
    cs=create_engine('mysql+pymysql://caoliang:8AEPAe5lw$#0i%p%@{}/creditservice?charset=utf8'.format(ip))
    cs=cs.connect()
    return cs

def dwi():
    wi=create_engine('mysql+pymysql://caoliang:8AEPAe5lw$#0i%p%@{}/withhold?charset=utf8'.format(ip))
    wi=wi.connect()
    return wi

def dsx():
    sx=create_engine('mysql+pymysql://caoliang:8AEPAe5lw$#0i%p%@{}/sx_info?charset=utf8'.format(ip))
    sx=sx.connect()
    return sx


def conn_close():
    dfq().close()
    ddw().close()
    dcc().close()
    dninefive().close()
    dhy().close()
    dcs().close()
    dwi().close()
    dsx().close()
