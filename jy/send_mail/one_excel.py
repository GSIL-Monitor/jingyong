import pandas as pd
from conn import *
from get_config import get_project






def one_sheet(sql,connect,project):
    excel_name, project_site, rec_phone, alarm, sender, receiver, chaosong= get_project(project)
    for i,j in enumerate(sql.split(';')):
        sql_data= pd.read_sql_query(j,connect)
        print(sql_data)
        sql_data.to_excel('{}/{}.xlsx'.format(project_site,excel_name[i]),index=False)
