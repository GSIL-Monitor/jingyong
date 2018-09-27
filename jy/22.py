import re
import pandas as pd
from sqlalchemy import create_engine

ninefive= create_engine('mysql+pymysql://otter:otter@123@10.253.169.47/da_base?charset=utf8')

conn=ninefive.connect()

data1=pd.read_sql('show procedure status where db="da_base"',conn)
# print(data1)
ss={}
for i in data1['Name'].tolist():
    print(i)
    content=pd.read_sql('show CREATE PROCEDURE %s '%i,conn)
    info=content['Create Procedure'][0]
    ss[i]=info

tables=['mis_guangqun',
'mis_highrish_salemanzx_everymonth',
'mis_highrisk_saleman_dl',
'mis_highrisk_saleman_zx',
'mis_highrisk_supplier',
'mis_loan_bysite',
'mis_loan_byterm_temp',
'collection_following',
'collection_following_cd',
'collection_following_cd_temp',
'collection_following_kf_temp',
'd_protocol_download_mistemp',
'db_current_dueday',
'loan_loan_writeoff',
'mid_monloan_detail',
'mis_approval_date_jieguo',
'mis_approval_date_jinjian',
'mis_approval_date_refuse2',
'mis_approval_date_shenpi',
'mis_approval_month_jieguo',
'mis_approval_month_jinjian',
'mis_approval_month_refuse2',
'mis_approval_nosigncontract',
'mis_approval_result_count',
'mis_approval_userdetail',
        ]
print('-------------------------------------------------------------------------------------')
for table in tables:
    for m,n in ss.items():
        if table in n:
            print(table,'>>>>>',m)
            continue
        else:
            print(table,'>>>>>')






