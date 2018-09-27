from sqlalchemy import create_engine
import pymysql
import os
conn=create_engine('mysql+pymysql://caoliang:8AEPAe5lw$#0i%p%@10.253.5.147/95_backup?charset=utf8')
conn=conn.connect()

job_list="""INSERT INTO batch_mis_log VALUES('预先数据准备','预先数据准备开始',NOW());      
        TRUNCATE TABLE batch_callall_log;
        INSERT INTO batch_callall_log VALUES(' ','proc_mis_batch_custtempapplyinfo开始',NOW());  
        CALL proc_mis_batch_custtempapplyinfo;/*cust_tempapply_info数据备份*/
        INSERT INTO batch_callall_log VALUES(' ','proc_mis_batch_ruleysoutengine开始',NOW());
        CALL proc_mis_batch_ruleysoutengine;/*rule_ysout_engine数据备份*/
        INSERT INTO batch_callall_log VALUES(' ','proc_mis_batch_ruleinengine开始',NOW());
        CALL proc_mis_batch_ruleinengine;/*rule_in_engine数据备份*/
        INSERT INTO batch_callall_log VALUES(' ','proc_mis_batch_ruleoutengine开始',NOW());
        CALL proc_mis_batch_ruleoutengine;/*rule_out_engine数据备份*/   
        INSERT INTO batch_callall_log VALUES(' ','proc_db_approval_result开始',NOW());
        CALL proc_db_approval_result(); /*记录审批结果*/
        INSERT INTO batch_callall_log VALUES(' ','tableau数据处理开始',NOW());
        CALL proc_mis_tableau_custtempapplyinfo(); #嗨钱事业部预审明细报表数据处理
        INSERT INTO batch_callall_log VALUES(' ','tableau数据处理结束',NOW());
        INSERT INTO batch_mis_log VALUES('预先数据准备','预先数据准备结束',NOW());   """

for job in job_list.split(';'):
    print(job)

    os.system('mysql -ucaoliang -p8AEPAe5lw$#0i%p% -h121.43.73.232 95_backup -e "show databases;"')
    # else:
    #     os.system('mysql -ucaoliang -p8AEPAe5lw$#0i%p% 95_backup -e ""'%job)
