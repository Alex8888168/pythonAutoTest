# _*_ coding: utf-8 _*_
# @Time     : 10/14/2022 8:40 AM
# @Author   : Alex Xu
# @Email    : Alexxu@apjcorp.com
# @File     : viewTest.py
# @Software : PyCharm

from common.handle_logging import log, log_file_path
from common.char_type_mapping import CharTypeMapping
from DataMigration.common.connect_mssql import connectMssql
from DataMigration.common.connect_mysql import connectMysql
from DataMigration.common.duplicate_removal import dup_rem
import hashlib

from module.view_autoField import test_view_autofield
from module.view_data import test_view_data
from module.view_field import test_view_field
from module.view_fieldDefault import test_view_field_default
from module.view_fieldEmpty import test_view_field_empty
from module.view_fieldTypeLen import test_view_field_typeAndLength

# 定义全局变量
# test_views数组存放后面用来测试的表
global test_views


def view_test():
    # # same_tables数组存放迁移成功的表
    # global same_tables, test_views
    # # test_tables数组存放后面用来测试的表
    # # 实际和same_tables的表是一样的，区别在于test_tables是按SQL Server有大写的表名，same_tables都是小写
    # global test_tables
    # # mysql_tables变量存放从MySQL指定数据库中获取到的所有table
    # global mysql_tables
    global test_views

    log.info("Start connect to the SQL Server.")
    mssql_data = connectMssql()
    mssql_conn = mssql_data[0]
    mssql_cursor = mssql_data[1]
    mssql_database = mssql_data[2]

    log.info("Start connect to the MySQL.")
    mysql_data = connectMysql()
    mysql_conn = mysql_data[0]
    mysql_cursor = mysql_data[1]
    mysql_database = mysql_data[2]

    if mssql_cursor and mysql_cursor:
        log.info("SQL Server and MySQL connect success.")
        mssql_views = []
        mssql_views_global = []
        # log.info("Execute \"select * from sys.views;\" to get all views from the \"" + mssql_database + "\" in SQL Server.")
        mssql_sql1 = "select * from sys.views;"
        mssql_cursor.execute(mssql_sql1)
        mssql_all_views = mssql_cursor.fetchall()
        print(len(mssql_all_views))

        for i in range(len(mssql_all_views)):
            mssql_views.append(mssql_all_views[i][0])
        mssql_views = sorted(mssql_views)
        # log.info("All views of the \"%s\" in SQL Server:%s", mssql_database, mssql_views)
        # --------------------------------------------------------
        mysql_views = []
        # log.info("Execute \"show table status where comment='view';\" to get all views from the \"" + mysql_database + "\" in MySQL.")
        mysql_sql1 = "show table status where comment='view';"
        mysql_cursor.execute(mysql_sql1)
        mysql_all_views = mysql_cursor.fetchall()
        print(len(mysql_all_views))

        for i in range(len(mysql_all_views)):
            # print(mysql_all_tables[i][0].lower())
            mysql_views.append(mysql_all_views[i][0])
        # print(mysql_views)
        mysql_views = sorted(mysql_views)
        # print(mysql_views)
        # log.info("All views of the \"%s\" in MySQL:%s", mysql_database, mysql_views)

        # 'lack_views' storage unmigrated views
        lack_views = []
        # 'extra_views' storage the extra views after the migration
        extra_views = []
        # 'same_views' storage the views of successful migration
        same_views = []
        # 'test_views' storage of test views
        test_views = []
        for i1 in range(len(mssql_views)):
            for j1 in range(len(mysql_views)):
                if mssql_views[i1] == mysql_views[j1]:
                    same_views.append(mssql_views[i1])
                    test_views.append(mssql_views[i1])
                    break
                else:
                    if j1 == len(mysql_views) - 1:
                        # mysql_views.append(mssql_views[i1])
                        lack_views.append(mssql_views[i1])
                    else:
                        continue
        # print(lack_views)
        if len(lack_views) > 0:
            log.error("These are %a unmigrated views:%s", len(lack_views), lack_views)

        for i2 in range(len(mysql_views)):
            for j2 in range(len(mssql_views)):
                if mysql_views[i2] == mssql_views[j2]:
                    break
                else:
                    if j2 == len(mssql_views) - 1:
                        extra_views.append(mysql_views[i2])
                    else:
                        continue
        # print(extra_views)
        if len(extra_views) > 0:
            log.error("These are %a extra view after migration:%s", len(extra_views), extra_views)

        if len(same_views) > 0:
            log.info("%a views that were successfully migrated:%s", len(same_views), same_views)

    else:
        log.error("SQL Server or MySQL connect fail.")

    # view field
    test_view_field(mssql_cursor, mysql_cursor, test_views)

    # view field type and length
    test_view_field_typeAndLength(mssql_cursor,mysql_cursor,test_views)

    # view field empty
    test_view_field_empty(mssql_cursor, mysql_cursor, test_views)

    # view field default
    test_view_field_default(mssql_cursor, mysql_cursor, test_views)

    # view automation field
    test_view_autofield(mssql_cursor, mysql_cursor, test_views)

    # view data
    test_view_data(mssql_cursor, mysql_cursor, test_views)

    mssql_conn.close()
    mysql_conn.close()
    dup_rem(log_file_path)

view_test()

# if __name__ == "__main__":
#     view_test()