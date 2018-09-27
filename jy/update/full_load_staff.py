import pandas as pd
from sqlalchemy import create_engine

from get_config import get_config
from update_current_load_time import update_current_time
from update_last_load_time import update_last_time


def full_load_staff():
    print('loading dim_staff.....')

    connTel = create_engine('mysql+pymysql://{}:{}@{}/{}?charset=utf8'.format(*get_config('telcms')))
    connDw = create_engine('mysql+pymysql://{}:{}@{}/{}?charset=utf8'.format(*get_config('dw_base')))
    connCc = create_engine('mysql+pymysql://{}:{}@{}/{}?charset=utf8'.format(*get_config('ccms2')))
    connFq = create_engine('mysql+pymysql://{}:{}@{}/{}?charset=utf8'.format(*get_config('fqmall_ht_prod')))

    connDw.execute("truncate table dim_staff")

    tel_sql = '''
        SELECT ID as id, USER_DEP_ID as department_id, USER_NAME as user_name, USER_REAL_NAME as user_real_name
        , USER_CODE as user_code, USER_ENABLED as user_enabled, 2 as user_type,
        '1700-01-01 00:00:00' as load_start, '2399-12-31 00:00:00' as load_end FROM tel_user

    '''
    ccms2_sql = '''
        SELECT  ID as id, USER_ORG_ID as department_id, USER_NAME as user_name, USER_REAL_NAME as user_real_name,
        USER_CODE as user_code, USER_ENABLED as user_enabled, 1 as user_type,
        '1700-01-01 00:00:00' as load_start, '2399-12-31 00:00:00' as load_end FROM pu_user

    '''

    fq_sql = '''
        SELECT  ID as id, USERNAME as user_name, REAL_NAME as user_real_name, ENABLED as user_enabled,
        3 as user_type,
        '1700-01-01 00:00:00' as load_start,'2399-12-31 00:00:00' as load_end FROM approve_user

    '''

    dic = {'id':'', 'user_name':'', 'user_code':'', 'user_type':'', 'user_enabled':'',
           'user_real_name':'', 'department_id':'', 'load_start':'', 'load_end':''}

    update_current_time('dim_staff')

    data_tel = pd.read_sql(tel_sql, connTel)
    ccms_sql = pd.read_sql(ccms2_sql, connCc)
    data_fq = pd.read_sql(fq_sql, connFq)

    print(len(data_tel), len(ccms_sql), len(data_fq))

    data_sql = pd.DataFrame(dic, index=[0])

    data_sql = data_sql.append([data_tel, ccms_sql, data_fq], ignore_index=[0])

    data_sql.drop([0], inplace=True)

    data_sql.to_sql('dim_staff', con=connDw, index=False, if_exists='append')

    update_last_time('dim_staff')
    print('loading dim_staff Done')


if __name__ == '__main__':
    full_load_staff()
