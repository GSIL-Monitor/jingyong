import pandas as pd
from conn import *
from get_config import get_project






def one_sheet(sql,connect,project):
    excel_name, project_site, rec_phone, alarm = get_project(project)
    sql_data= pd.read_sql_query(sql,connect)
    sql_data.to_excel('{}/{}.xlsx'.format(project_site,excel_name))
