import os
import lightgbm as lbm
from sqlalchemy import create_engine


conn=create_engine('mysql+pymysql://root:111@localhost')






lbm.create_tree_digraph()

name='shceduler'
print(type(os.system('ps -ef | grep %s | grep -v grep | wc -l'%name )))
if os.system('ps -ef | grep %s | grep -v grep | wc -l'%name )==0:
    os.system("nohup airflow scheduler >> /home/jiankong/restart.log 2>&1")
    print(1)
else:
    print('进程还活着')
