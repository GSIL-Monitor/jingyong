# import datetime
import gc
import pandas as pd
from sqlalchemy import create_engine
from get_config import get_config
from update_current_load_time import update_current_time
from update_last_load_time import update_last_time

# from sms import sendto as cs


def full_load_fact_order_verify():
    connFq = create_engine('mysql+pymysql://{}:{}@{}/{}?charset=utf8'.format(*get_config('fqmall_ht_prod')))
    connDw = create_engine('mysql+pymysql://{}:{}@{}/{}?charset=utf8'.format(*get_config('dw_base')))
    connHa = create_engine('mysql+pymysql://{}:{}@{}/{}?charset=utf8'.format(*get_config('hy_hawkeye')))

    mis_dic_custfrom_sql = '''
        select CUSTFROM_CODE,CUSTFROM_NAME as ver_cust_from from mis_dic_custfrom'''
    # 进件来源数据
    print("reading dw_base.mis_dic_custfrom_data ...")
    mis_dic_custfrom_data = pd.read_sql(mis_dic_custfrom_sql, connDw)
    print(len(mis_dic_custfrom_data))
    print("readed mis_dic_custfrom_data is over ... done")

    d_application_pay_sql = '''
        SELECT app_application_no, app_creditproduct_id, app_cust_type, date(app_approval_starttime) as first_begin_date
        ,time(app_approval_starttime) as first_begin_time
        ,date(app_approval_endtime) as first_end_date, time(app_approval_endtime) as first_end_time
        ,first_denying_code, app_approval_name, date(app_face_starttime) as final_begin_date
        ,time(app_face_starttime) as final_begin_time, app_face_name
        ,date(app_face_endtime) as final_end_date, time(app_face_endtime) as final_end_time
        ,app_approval_desc, app_check_desc, faceResult, app_face_refause
        ,app_check_name, date(app_check_endtime) as check_date, time(app_check_endtime) as check_time
        ,approval_pro_end_name, date(approval_pro_end_date) as app_date, time(approval_pro_end_date) as app_time
        ,app_username, app_contract_refause as ver_contract_refause, app_contract_name as ver_contract_name
        ,app_delivery_refause as ver_delivery_refause, app_contract_endtime as ver_merchant_endtime
        ,hy_industry_code, create_date as ver_create_date,cust_from, app_privilege_name as ver_privilege_name
        FROM d_application_pay
    '''

    print("reading d_application_pay_data ...")
    update_current_time('fact_order_verify')
    # 订单数据
    d_application_pay_data = pd.read_sql(d_application_pay_sql, connFq)

    print(len(d_application_pay_data))
    print("reading d_application_pay_data ... Done")

    # 阿波罗审核结果
    apolo_result_sql = '''
    SELECT ab.APP_NO,ab.FACRESULT FROM as_rule_execute_result ab,(
		SELECT a.APP_NO, MAX(a.CREATE_TIME) AS ctime FROM as_rule_execute_result a GROUP BY a.APP_NO) ac
    WHERE ab.app_no = ac.app_no
    AND ab.create_time = ac.ctime
    '''

    print("reading as_rule_execute_result ...")
    apolo_result_data = pd.read_sql(apolo_result_sql, connHa)

    print("reading as_rule_execute_result ...Done")

    data_apolo = pd.merge(d_application_pay_data, apolo_result_data,
                          left_on='app_application_no', right_on='APP_NO', how='left') \
        .drop(['APP_NO'], axis=1)

    print(len(data_apolo))
    del apolo_result_data, d_application_pay_data
    gc.collect()

    # 反欺诈审核结果
    fqz_result_sql = '''
        select a.app_no, fqz_facresult from rule_freein_engine a,
        (select app_no ,max(create_time) as cre from rule_freein_engine group by app_no
        ) b where a. app_no = b. app_no and a.create_time = b.cre
    '''

    print("reading rule_freein_engine ...")
    fqz_result_data = pd.read_sql(fqz_result_sql, connFq)
    print("reading rule_freein_engine ... Done")
    data_fqz = pd.merge(data_apolo, fqz_result_data,
                        left_on='app_application_no', right_on='app_no', how='left') \
        .drop(['app_no'], axis=1)

    data_fqz = data_fqz.drop_duplicates(['app_application_no'])
    print(len(data_fqz))
    del fqz_result_data, data_apolo
    gc.collect()

    # 资金方审核结果
    inv_result_sql = '''
        SELECT DISTINCT hy_application_no, status from inv_application_pay
    '''
    print("reading inv_application_pay ...")
    inv_result_data = pd.read_sql(inv_result_sql, connFq)
    print("reading inv_application_pay ...Done")
    data_inv = pd.merge(data_fqz, inv_result_data,
                        left_on='app_application_no', right_on='hy_application_no', how='left') \
        .drop(['hy_application_no'], axis=1)

    print(len(data_inv))
    del inv_result_data, data_fqz
    gc.collect()

    # 资金方ID和贷款金额
    loan_loan_sql = '''
        SELECT DISTINCT ID , AMOUNT, APPLICATION_ID from loan_loan
    '''
    print("reading loan_loan ...")
    loan_loan_data = pd.read_sql(loan_loan_sql, connFq)
    print("reading loan_loan ...Done")
    data_loan_loan = pd.merge(data_inv, loan_loan_data,
                              left_on='app_application_no', right_on='APPLICATION_ID', how='left').drop(['APPLICATION_ID'], axis=1)

    print(len(data_loan_loan))
    del loan_loan_data, data_inv
    gc.collect()

    # product id

    loan_loan_acc_sql = '''
        SELECT DISTINCT LOAN_ID , PRODUCT_ID  from loan_loan_acc
    '''
    print("reading loan_loan_acc ...")
    loan_loan_acc_data = pd.read_sql(loan_loan_acc_sql, connFq)
    print("reading loan_loan_acc ...Done")

    data_acc = pd.merge(data_loan_loan, loan_loan_acc_data,
                        left_on='ID', right_on='LOAN_ID', how='left') \
        .drop(['ID', 'LOAN_ID'], axis=1)

    print(len(data_acc))
    del loan_loan_acc_data, data_loan_loan
    gc.collect()

    # 资金方
    loan_product_sql = '''
        SELECT DISTINCT ID , INVESTOR_NAME  from loan_product
    '''
    print("reading loan_product ...")
    loan_product_data = pd.read_sql(loan_product_sql, connFq)
    print("reading loan_product ...Done")
    data_product = pd.merge(data_acc, loan_product_data,
                            left_on='PRODUCT_ID', right_on='ID', how='left') \
        .drop(['ID', 'PRODUCT_ID'], axis=1)

    print(len(data_product))
    del loan_product_data, data_acc
    gc.collect()

    # 用户查询
    customer_sql = '''
        SELECT DISTINCT user_name, max(customer_key) from dim_customer group by user_name
    '''

    print("reading dim_customer ...")
    customer_data = pd.read_sql(customer_sql, connDw)
    print("reading dim_customer ...Done")

    data_customer = pd.merge(data_product, customer_data,
                             left_on='app_username', right_on='user_name', how='left') \
        .drop(['user_name'], axis=1)

    print(len(data_customer))
    del customer_data, data_product
    gc.collect()

    # 日期查询
    date_sql = '''
        SELECT date_value, date_key from dim_date
    '''
    print("reading dim_date ...")
    date_data = pd.read_sql(date_sql, connDw)
    print("reading dim_date ...Done")
    time_sql = '''
        SELECT time_value, time_key from dim_time
    '''
    print("reading dim_time ...")
    time_data = pd.read_sql(time_sql, connDw)
    print("reading dim_time ...Done")

    # date查询
    data_first_begin_date = pd.merge(data_customer, date_data,
                                     left_on='first_begin_date', right_on='date_value', how='left') \
        .drop(['first_begin_date', 'date_value'], axis=1)

    data_first_end_date = pd.merge(data_first_begin_date, date_data,
                                   left_on='first_end_date', right_on='date_value', how='left',
                                   suffixes=['_first_begin', '_first_end']) \
        .drop(['first_end_date', 'date_value'], axis=1)

    data_final_begin_date = pd.merge(data_first_end_date, date_data,
                                     left_on='final_begin_date', right_on='date_value', how='left') \
        .drop(['final_begin_date', 'date_value'], axis=1)
    data_final_end_date = pd.merge(data_final_begin_date, date_data,
                                   left_on='final_end_date', right_on='date_value', how='left',
                                   suffixes=['_final_begin', '_final_end']) \
        .drop(['final_end_date', 'date_value'], axis=1)
    data_check_date = pd.merge(data_final_end_date, date_data,
                               left_on='check_date', right_on='date_value', how='left') \
        .drop(['check_date', 'date_value'], axis=1)
    data_app_date = pd.merge(data_check_date, date_data,
                             left_on='app_date', right_on='date_value', how='left',
                             suffixes=['_check', '_app']) \
        .drop(['app_date', 'date_value'], axis=1)

    del date_data, data_first_begin_date, data_first_end_date, data_final_begin_date, data_final_end_date, data_check_date

    # time查询
    time_first_begin_date = pd.merge(data_app_date, time_data,
                                     left_on='first_begin_time', right_on='time_value', how='left') \
        .drop(['first_begin_time', 'time_value'], axis=1)

    time_first_end_date = pd.merge(time_first_begin_date, time_data,
                                   left_on='first_end_time', right_on='time_value', how='left',
                                   suffixes=['_first_begin', '_first_end']) \
        .drop(['first_end_time', 'time_value'], axis=1)

    time_final_begin_date = pd.merge(time_first_end_date, time_data,
                                     left_on='final_begin_time', right_on='time_value', how='left') \
        .drop(['final_begin_time', 'time_value'], axis=1)
    tme_final_end_date = pd.merge(time_final_begin_date, time_data,
                                  left_on='final_end_time', right_on='time_value', how='left',
                                  suffixes=['_final_begin', '_final_end']) \
        .drop(['final_end_time', 'time_value'], axis=1)
    time_check_date = pd.merge(tme_final_end_date, time_data,
                               left_on='check_time', right_on='time_value', how='left') \
        .drop(['check_time', 'time_value'], axis=1)
    time_app_date = pd.merge(time_check_date, time_data,
                             left_on='app_time', right_on='time_value', how='left',
                             suffixes=['_check', '_app']) \
        .drop(['app_time', 'time_value'], axis=1)

    del time_data, time_first_begin_date, time_first_end_date, time_final_begin_date, tme_final_end_date, time_check_date

    # 员工查询
    staff_sql = '''
        SELECT user_name, max(staff_key) from dim_staff group by user_name
    '''
    print("reading dim_staff ...")
    staff_first_data = pd.read_sql(staff_sql, connDw)
    print("reading dim_staff ...Done")
    data_staff_first = pd.merge(time_app_date, staff_first_data,
                                left_on='app_approval_name', right_on='user_name', how='left') \
        .drop(['app_approval_name', 'user_name'], axis=1)

    data_staff_final = pd.merge(data_staff_first, staff_first_data,
                                left_on='app_face_name', right_on='user_name', how='left') \
        .drop(['app_face_name', 'user_name'], axis=1)

    dic_rename = {'app_application_no': 'ver_number', 'app_creditproduct_id': 'ver_creditproduct_id',
                  'app_cust_type': 'ver_cust_type'
        , 'first_denying_code': 'ver_first_denying_code'
        , 'app_approval_desc': 'ver_frist_verify_desc', 'app_check_desc': 'ver_check_desc',
                  'faceResult': 'ver_final_result'
        , 'app_face_refause': 'ver_face_refause'
        , 'app_check_name': 'ver_check_staff', 'approval_pro_end_name': 'ver_approval_staff',
                  'FACRESULT': 'ver_apollo_result'
        , 'fqz_facresult': 'ver_fqz_result'
        , 'status': 'ver_finance_result', 'AMOUNT': 'ver_amount', 'INVESTOR_NAME': 'ver_finance_key',
                  'max(customer_key)': 'ver_user_key'
        , 'date_key_first_begin': 'ver_frist_begin_date_key'
        , 'date_key_first_end': 'ver_frist_end_date_key', 'date_key_final_begin': 'ver_final_begin_date_key'
        , 'date_key_final_end': 'ver_final_end_date_key', 'date_key_check': 'ver_check_date_key'
        , 'date_key_app': 'ver_approval_date_key', 'time_key_first_begin': 'ver_frist_begin_time_key',
                  'time_key_first_end': 'ver_frist_end_time_key'
        , 'time_key_final_begin': 'ver_final_begin_time_key'
        , 'time_key_final_end': 'ver_final_end_time_key', 'time_key_check': 'ver_check_time_key',
                  'time_key_app': 'ver_approval_time_key'
        , 'max(staff_key)_x': 'ver_frist_user_key'
        , 'max(staff_key)_y': 'ver_final_user_key'}

    del staff_first_data, data_staff_first
    gc.collect()

    data_staff_final.rename(columns=dic_rename, inplace=True)

    print(len(data_staff_final))

    # 设置新老客户，VIP贷
    product_sql = '''
            select product_type_key as ver_user_type, product_code from dim_product_type
    '''
    print("loading product_sql ...")
    product_data = pd.read_sql(product_sql, connDw)
    data_product = pd.merge(data_staff_final, product_data, left_on='hy_industry_code',
                            right_on='product_code', how='left')
    print("loading product_sql Done")
    print(len(data_product))

    # 关联身份证号
    cust_sql = '''
        select DISTINCT username, IDENTITY_NO from cust_customer
    '''
    cust_data = pd.read_sql(cust_sql, connFq)
    data_product = pd.merge(data_product, cust_data, left_on='app_username',
                            right_on='username', how='left')

    # loan_loan_acc转成身份证号
    loan_loan_acc_sql = '''
        select cc.IDENTITY_NO as id_no, min(lla.CREATE_TIME) as min_create from loan_loan_acc as lla 
        LEFT JOIN cust_customer as cc on lla.USERNAME = cc.USERNAME
        where lla.CREATE_TIME is not null and cc.IDENTITY_NO is not null
        GROUP BY cc.IDENTITY_NO
    '''

    loan_loan_acc_data = pd.read_sql(loan_loan_acc_sql, connFq)

    data_product = pd.merge(data_product, loan_loan_acc_data, left_on='IDENTITY_NO',
                            right_on='id_no', how='left')

    condition = (data_product['id_no'] == data_product['IDENTITY_NO']) & (data_product['product_code'] == 'MDCP') \
                & (data_product['ver_create_date'] > data_product['min_create'])

    MDOH_key = product_data[product_data['product_code'] == 'MDOH'].iloc[0, 0]

    data_product.loc[condition, 'ver_user_type'] = MDOH_key

    data_product.drop(['hy_industry_code', 'product_code', 'IDENTITY_NO', 'id_no', 'app_username', 'username',
                       'min_create'], axis=1, inplace=True)

    # data_product = data_product.drop_duplicates(['ver_number'])

    # 转化进件来源
    date_final = pd.merge(data_product, mis_dic_custfrom_data, left_on='cust_from', right_on='CUSTFROM_CODE',
                          how='left').drop(['cust_from', 'CUSTFROM_CODE'], axis=1)

    print('MDOH_key  Done')
    print(len(date_final))
    print("writing  ...")

    connDw.execute("truncate table fact_order_verify")
    date_final.sort_values(by=['ver_number','ver_approval_date_key','ver_merchant_endtime'],inplace=True)
    date_final=date_final.drop_duplicates(['ver_number'], keep = 'last')
    date_final.to_sql('fact_order_verify', con=connDw, index=False, if_exists='append', chunksize=500)
    print("writing  ...Done")
    update_last_time('fact_order_verify')


if __name__ == '__main__':
    # cs(['17721291792', '15822708861', '18301959085'], content='审核表定时任务开始')
    full_load_fact_order_verify()
    # cs(['17721291792', '15822708861', '18301959085'], content='审核表定时任务完成，恭喜！！')






