# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

import pandas as pd
import yagmail
import sys
import time

from util import conn_db

def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')

    # See PyCharm help at https://www.jetbrains.com/help/pycharm/
    #创建数据库连接对象
    impala_142 = conn_db.IMPALA('172.21.3.142',21050,'admin','admin','dwd')

    # 昨日诊股次数3次，且没有开通诊股白名单的未付费用户
    sql1 = '''select a.yx_id from 
                    (select yx_id ,COUNT(1) diag_times from dwd.dwd_app_diag_stock_detail
                    where choose_type != '热股推荐'
                    and cast(begin_day_id as string) BETWEEN  from_unixtime(unix_timestamp(days_sub(NOW() ,1)),'yyyyMMdd') and from_unixtime(unix_timestamp(days_sub(NOW() ,1)),'yyyyMMdd')
                    group by yx_id 
                    HAVING COUNT(1) =3)a
                    left join 
                    (-- 诊股白名单用户
                    select  distinct 
                    CONCAT('yx', case WHEN a.user_no rlike '(yx)[0-9]' then substr(a.user_no ,3,10) else a.user_no end) yx_id
                    from ods.ods_yixueweb_tool_whitelist a
                    where code = 7)b on a.yx_id = b.yx_id
                    left join dwd.dwd_reg_user_detail c on a.yx_id = c.yx_id 
                    where b.yx_id is null 
                    and c.whether_pay_bigger_than_0 = '0' '''

    # 执行sql，查询数据
    sql_result = pd.DataFrame(impala_142.ExecQuery(sql1))

    # 给列命名
    sql_result.columns = ['yx_id']
    cur_time = time.strftime('%Y_%m%d_%H%M')

    # 输出到excel
    file_path = 'C:/Users/chulang/Desktop/diag_stock_user_yesterday.xlsx'
    sql_result.to_excel(file_path)