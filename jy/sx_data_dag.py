import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta,datetime
import time


#-------------------------------------------------------------------------------
# these args will get passed on to each operator
# you can override them on a per-task basis during operator initialization

default_args = {
    'owner': 'caoliang',
    'depends_on_past': False,
    'start_date': datetime(2018, 6, 12,0,0,1),
    'email': ['335095199@qq.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=4),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2018, 5, 10),
    # 'max_active_runs':1,
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'adhoc':False,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'trigger_rule': u'all_success'
}

#-------------------------------------------------------------------------------
# dag

dag = DAG(
    'sx_data',
    default_args=default_args,
    description='inc data DAG',
    schedule_interval='* 1 * * *' )


move_sx_data = BashOperator(
    task_id='sx_data',
    bash_command='python /home/cl/scrip/sx_data.py',
    dag=dag)




move_sx_data
