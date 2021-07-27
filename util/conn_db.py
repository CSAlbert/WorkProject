# *************************   连接impala的工具类   *************************
from impala.dbapi import connect

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


# *************************   连接Mysql的工具类   *************************

import pymysql

class MysqlDB:
    cursor = ''  # 句柄
    db = ''  # 打开数据库连接
    '''
        定义构造方法
        host：主机名
        username;用户名
        password:密码
        dbname:数据库名
        db:打开数据库连接
        cursor:获取游标句柄
    '''

    def __init__(self, host, user, password, database):

        self.host = host
        self.user = user
        self.password = password
        self.database = database

        self.db = pymysql.connect(host=self.host, user=self.user, password=self.password, database=self.database)
        self.cursor = self.db.cursor()

    # 获取所有的结果集
    def getAllResult(self, sql):
        self.cursor.execute(sql)
        results = self.cursor.fetchall()
        return results

    # 获取所有的结果集
    def getSignleResult(self, sql):
        self.cursor.execute(sql)
        results = self.cursor.fetchone()
        return results

    # 插入或更新数据
    def insertOrUdateInfo(self, sql):
        try:
            # 执行SQL语句
            self.cursor.execute(sql)
            # 提交到数据库执行
            self.db.commit()
        except:
            # 发生错误时回滚
            self.db.rollback()
        # 返回受影响的行数
        return self.cursor.rowcount

    # 关闭链接
    def close(self):
        self.db.close()