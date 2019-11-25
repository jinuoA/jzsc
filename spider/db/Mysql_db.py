# _*_coding:utf-8_*_
# Author by Mr.Xu

import pymysql
from spider.db.config import *


class MySQLClient(object):

    def __init__(self, host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASSWORD, database=DATA_BASE):
        """
        初始化
        :param host: Mysql 地址
        :param port: Mysql 端口
        :param user: Mysql 用户名
        :param password: Mysql 密码
        """
        try:
            self._db = pymysql.connect(host=host, port=port, user=user, password=password, database=database,
                                       charset='utf8')
            self._cursor = self._db.cursor()
            self._cursor.execute('SELECT VERSION()')
            data = self._cursor.fetchone()
            print('Database version: ', data)
        except:
            print('Database conn failed.')

    def __createDataBase__(self, database=DATA_BASE):
        """
        创建数据库
        :param DataBaseName: 数据库名称
        :return:
        """
        self._cursor.execute('drop table if exists {0}'.format(database))
        self._cursor.execute('CREATE DATABASE {0} DEFAULT  CHARACTER SET  utf8'.format(database))

    def __createTable__(self, sql_path=SQL_PATH):
        """
        创建数据表
        :param sql_path: sql文件路劲
        :return: 添加结果
        """
        try:
            with open(sql_path, mode='r', encoding='utf-8') as f:
                sql = f.read()
                self._cursor.execute(sql)
                self._db.commit()
            f.close()
        except Exception as e:
            print(e, '创建数据表失败，请重试')
            return None

    def __insertData__(self, table_name, data):
        keys = ', '.join(data.keys())
        values = ', '.join(['%s'] * len(data))
        sql = "INSERT INTO {table_name}({keys}) VALUES ({values})".format(table_name=table_name, keys=keys,
                                                                          values=values)
        try:
            self._db.ping()
            if self._cursor.execute(sql, tuple(data.values())):
                print('数据保存成功')
                self._db.commit()
                return True
        except Exception as e:
            print(e, '数据保存失败')
            self._db.rollback()
            return False


    def getAll(self, sql, param=None):
        """
        @summary: 执行查询，并取出所有结果集
        @param sql:查询ＳＱＬ，如果有查询条件，请只指定条件列表，并将条件值使用参数[param]传递进来
        @param param: 可选参数，条件列表值（元组/列表）
        @return: result list(字典对象)/boolean 查询到的结果集
        """
        if param is None:
            count = self._cursor.execute(sql)
        else:
            count = self._cursor.execute(sql, param)
        if count > 0:
            result = self._cursor.fetchall()
        else:
            result = False
        return result

    # def __insertManyData__(self, table_name, datas):
    #
    #     keys = ', '.join(datas[0].keys())
    #     usersvalues = []
    #     for data in datas:
    #         usersvalues.append((tuple(data.values())))  # 注意要用两个括号扩起来
    #
    #     try:
    #         self._db.ping()
    #         if self._cursor.executemany("insert into {table_name}({keys}) values(%s,%s,%s,%s)", usersvalues):
    #             print('数据保存成功')
    #             self._db.commit()
    #             return True
    #     except Exception as e:
    #         print(e, '数据保存失败')
    #         self._db.rollback()
    #         return False

    def __closeDB__(self):
        self._db.close()


if __name__ == '__main__':
    conn = MySQLClient()
    conn.__createTable__()
    conn.__closeDB__()
