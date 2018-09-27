# -*- coding: utf-8 -*-

import requests
import pandas as pd
from sqlalchemy import create_engine
import datetime

# from app import logger

SMS_USER = 'N9711284'
SMS_PASSWORD = 'woavp9cJq'
SMS_HOST = 'http://smssh1.253.com/msg/send/json'
conn=create_engine('mysql+pymysql://root:shitou@10.253.170.0/airflow?charset=utf8').connect()

class SMSProvider(object):
    def __init__(self): pass

    def sendto(self, mobile, content): pass


class ChuangLanSMSProvider(SMSProvider):
    def __init__(self):
        super(ChuangLanSMSProvider, self).__init__()
        self.data = {'account': SMS_USER, 'password': SMS_PASSWORD}
        self.conn=create_engine('mysql+pymysql://root:shitou@10.253.170.0/airflow?charset=utf8').connect()

    def sendto(self, mobile, content):
        if isinstance(mobile, list):
            mobile = ','.join(mobile)
        self.data.update(phone=mobile, msg=content, uid=1)

        try:
            res = requests.post(SMS_HOST, json=self.data)
            if res.status_code != 200:
                return False

            res = res.json()
            return res.get('code', None) == '0'
        except Exception as e:
            print(11)
            # logger.exception('chuanglan send error: {%s: %s}' % (mobile, content))
            return False

    def airflow(self,conn):
        sql = """select dag_id,date_format(execution_date,'%%Y-%%m-%%d %%H:%%m:%%s') date,state from dag_run where DATE(execution_date)=DATE_sub(DATE(now()),INTERVAL 1 DAY )"""
        data = pd.read_sql(sql, self.conn)
        useless = pd.DataFrame({'dag_id': [None, None], 'date': [None, None], 'state': ['failed', 'running']})
        data = pd.concat([data, useless])
        running = set(data[data['state'] == 'running'].dag_id.tolist())
        success = set(data[data['state'] == 'success'].dag_id.tolist())
        failed = set(data[data['state'] == 'failed'].dag_id.tolist())

        all_num="""select count(*) from dag WHERE is_paused=0"""
        content = """
        今日定时任务截止到%s，任务运行情况快报：
        \n正在运行的任务:%s\n
        已经完成的任务:%s\n
        失败的任务:%s
        """ % (datetime.datetime.now(),running, success, failed)
        print(content)
        return content

# if __name__ == '__main__':
#     p = ChuangLanSMSProvider()
#     p.sendto(['17721291792','15822708861','18301959085'], content=p.airflow(conn))#'15822708861',,'18301959085'