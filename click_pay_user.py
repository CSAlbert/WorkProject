from impala.dbapi import connect
import pandas as pd
import sys
import time
import yagmail
import os

# *************************   连接impala的工具类   *************************
class IMPALA:
    def __init__(self,host,port,user,pwd,db):
        self.host = host
        self.port = port
        self.user = user
        self.pwd = pwd
        self.db = db

    def __GetConnect(self):
        if not self.db:
            raise(NameError,"没有设置数据库信息")
        self.conn = connect(host=self.host,port=self.port,user=self.user,password=self.pwd,database=self.db)

        cur = self.conn.cursor()
        if not cur:
            raise(NameError,"连接数据库失败")
        else:
            return cur

    def ExecQuery(self,sql):
        cur = self.__GetConnect()
        cur.execute(sql)
        resList = cur.fetchall()

        #查询完毕后必须关闭连接
        self.conn.close()
        return resList

    def ExecNonQuery(self,sql):
        cur = self.__GetConnect()
        cur.execute(sql)
        self.conn.commit()
        self.conn.close()
    
# *************************   发送邮件  *************************
def send_email(email_address,email_subject,file_path) :
    # 链接邮箱服务器
    yag = yagmail.SMTP( user="chulang@yxcps.com", password="yxcps@1357", host='smtp.qiye.163.com')

    # 收件人邮箱地址
    # email_address = ['1350930355@qq.com']

    # 邮件主题 
    # subject = '当日未付费用户诊股明细'+file_name

    # 附件地址
    # excel_file = 'C:/Users/chulang/Desktop/zhengu_'+file_name+'.xlsx'

    # 邮件正文
    contents = ['具体数据请查看附件',file_path]

    # 发送邮件
    yag.send(email_address, email_subject, contents)
    print('成功发送邮件！')

# *************************   删除windows上文件  *************************
def delete_file(file_path):
    if os.path.exists(file_path):
        #删除文件，可使用以下两种方法。
        os.remove(file_path) # 则删除
        #os.unlink(my_file)
        print('成功删除文件！')
    else:
        print('no such file:%s'%file_path)

if __name__ == '__main__':

    #创建数据库连接对象
    impala_142 = IMPALA('172.21.3.142',21050,'admin','admin','ods')

    # 点击购买按钮用户付费情况
    sql = '''select c.yx_id,a.date_time,product_name,teacher_name,c.whether_pay_bigger_than_0 ,IF(d.yx_id is not null ,1,0) whether_app_order ,b.click_times,c.reg_date from 
            (--APP商城中点击购买按钮
            select zg_id ,from_unixtime(begin_date) date_time,event_name ,begin_day_id ,cus1 product_name,cus5 teacher_name ,ROW_NUMBER() over(PARTITION by zg_id order by begin_date desc) rk 
            from ods.ods_zhuge_b_user_event_attr_3 a
            where event_name = 'click_pay_button')a
            join (select zg_id ,COUNT(1) click_times from ods.ods_zhuge_b_user_event_attr_3 a
            where event_name = 'click_pay_button' group by zg_id)b on a.zg_id = b.zg_id
            left join dwd.dwd_reg_user_detail c on a.zg_id = c.zg_id
            left join (select distinct CONCAT('yx',kh_no) yx_id from ods.ods_yixuewuliu_wl_order
            where order_no like '%XT%'
            and order_status = '17') d on c.yx_id = d.yx_id
            where a.rk = 1
            order by a.date_time desc '''

    # 执行sql，查询数据
    sql_result = pd.DataFrame(impala_142.ExecQuery(sql))
    
    # 给列命名
    sql_result.columns = ['yx_id','最近一次点击购买的时间','商品名称','所属老师','是否付费','是否在app付费','点击购买总次数','注册时间']

    cur_time = time.strftime('%Y_%m%d_%H%M')

    # 输出到excel
    file_path = 'C:/Users/chulang/Desktop/click_pay_user_in_app.xlsx'
    sql_result.to_excel(file_path)
    
    #发送邮件
    # 收件人邮箱地址
    email_address = ['1350930355@qq.com']
    # 邮件主题 
    email_subject = '点击购买按钮用户付费情况'
    send_email(email_address,email_subject,file_path)

    #删除文件
    delete_file(file_path)