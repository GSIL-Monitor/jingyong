import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta,datetime
import time
import sys



#-------------------------------------------------------------------------------
# these args will get passed on to each operator
# you can override them on a per-task basis during operator initialization

default_args = {
    'owner': 'caoliang',
    'depends_on_past': False,
    'start_date': datetime(2018, 6, 22,6),
    'retries': 2,
    'retry_delay': timedelta(minutes=4),
}

dag = DAG(
    'inc_data',
    default_args=default_args,
    description='inc data DAG',
    schedule_interval='0 6 * * *' )


move_order = BashOperator(
    task_id='fact_order_application',
    bash_command='python /home/cl/scrip/update/inc_fact_order_application.py',
    dag=dag)


move_customer = BashOperator(
    task_id='customer',
    bash_command='python /home/cl/scrip/update/inc_dim_customer.py',
    dag=dag)

move_staff = BashOperator(
    task_id='staff',
    bash_command='python /home/cl/scrip/full_load_staff.py',
    dag=dag)

move_inc_load_fact_order_verify_insert = BashOperator(
    task_id='inc_load_fact_order_verify_insert',
    bash_command='python /home/cl/scrip/full_load_fact_order_verify.py',
    dag=dag)

move_fact_loan=BashOperator(
    task_id='fact_loan',
    bash_command='python /home/cl/scrip/update/inc_fact_loan.py',
    dag=dag
)


move_fact_tocash=BashOperator(
    task_id='fact_tocash',
    bash_command='python /home/cl/scrip/update/inc_fact_tocash.py',
    dag=dag
)

move_fact_protocol_info=BashOperator(
    task_id='fact_protocol_info',
    bash_command='python /home/cl/scrip/update/inc_fact_protocol_info.py',
    dag=dag
)


move_staff >> move_fact_loan

move_customer >> move_order

move_inc_load_fact_order_verify_insert.set_upstream(move_staff)
move_inc_load_fact_order_verify_insert.set_upstream(move_customer)

move_fact_tocash

move_fact_protocol_info

