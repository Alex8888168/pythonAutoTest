# _*_ coding: utf-8 _*_
# @Time     : 10/22/2022 3:02 PM
# @Author   : Alex Xu
# @Email    : Alexxu@apjcorp.com
# @File     : all_test.py
# @Software : PyCharm

from DataMigration.module.table_autoField import test_table_autofield
from DataMigration.module.table_data import test_table_data
from DataMigration.module.table_field import  test_table_field
from DataMigration.module.table_fieldEmpty import test_table_field_empty
from DataMigration.module.table_fieldTypeLen import test_table_field_typeAndLength
from DataMigration.module.table_index import test_table_index
from DataMigration.module.table_pri import test_table_pri
from DataMigration.module.table_fieldDefault import test_table_field_default
from DataMigration.common.handle_logging import log, log_file_path
from DataMigration.common.connect_mssql import connectMssql
from DataMigration.common.connect_mysql import connectMysql
from common.duplicate_removal import dup_rem
from module.view_autoField import test_view_autofield
from module.view_data import test_view_data
from module.view_field import test_view_field
from module.view_fieldDefault import test_view_field_default
from module.view_fieldEmpty import test_view_field_empty
from module.view_fieldTypeLen import test_view_field_typeAndLength

# Define global variables
# 'test_tables' Stores the tables that have been migrated successfully
global test_tables
# 'test_views' Stores the views that have been migrated successfully
global test_views

def all_test():
    global test_tables, test_views
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
        # -------tables-------------------------------------------------
        # log.info("Define an empty array of \"mssql_tables\" to store all tables for a specified SQL Server database.")
        mssql_tables = []
        mssql_tables_global = []

        mssql_sql1 = "select * from sys.tables;"
        mssql_cursor.execute(mssql_sql1)
        mssql_all_tables = mssql_cursor.fetchall()
        for i in range(len(mssql_all_tables)):
            mssql_tables_global.append(mssql_all_tables[i][0])
            mssql_tables.append(mssql_all_tables[i][0])
        mssql_tables = sorted(mssql_tables)
        mssql_tables_global = sorted(mssql_tables_global)
        mysql_tables = []
        mysql_sql1 = "show full tables where Table_Type = 'BASE TABLE';"
        mysql_cursor.execute(mysql_sql1)
        mysql_all_tables = mysql_cursor.fetchall()

        for i in range(len(mysql_all_tables)):
            mysql_tables.append(mysql_all_tables[i][0])
        mysql_tables = sorted(mysql_tables)
        # log.info("All tables of the \"" + mysql_database + "\" in MySQL:%s", mysql_tables)

        # 'lack_tables' storage unmigrated tables
        lack_tables = []
        # 'extra_tables' storage the extra tables after the migration
        extra_tables = []
        # 'same_tables' storage the table of successful migration
        same_tables = []
        # 'test_tables' storage of test tables
        test_tables = []

        for i1 in range(len(mssql_tables)):
            for j1 in range(len(mysql_tables)):
                if mssql_tables[i1].lower() == mysql_tables[j1]:
                    same_tables.append(mssql_tables[i1])
                    test_tables.append(mssql_tables_global[i1])
                    break
                else:
                    if j1 == len(mysql_tables) - 1:
                        lack_tables.append(mssql_tables_global[i1])
                    else:
                        continue
        if len(lack_tables) > 0:
            log.error("These are %a unmigrated tables:%s", len(lack_tables), lack_tables)
        for i2 in range(len(mysql_tables)):
            for j2 in range(len(mssql_tables)):
                if mysql_tables[i2] == mssql_tables[j2].lower():
                    break
                else:
                    if j2 == len(mssql_tables) - 1:
                        extra_tables.append(mysql_tables[i2])
                    else:
                        continue
        if len(extra_tables) > 0:
            log.error("These are %a extra table after migration:%s", len(extra_tables), extra_tables)
        if len(same_tables) > 0:
            log.info("%a tables that were successfully migrated:%s", len(same_tables), same_tables)

        # --------views------------------------------------------------
        mssql_views = []
        mssql_views_global = []
        # log.info("Execute \"select * from sys.views;\" to get all views from the \"" + mssql_database + "\" in SQL Server.")
        mssql_sql1 = "select * from sys.views;"
        mssql_cursor.execute(mssql_sql1)
        mssql_all_views = mssql_cursor.fetchall()

        for i in range(len(mssql_all_views)):
            mssql_views.append(mssql_all_views[i][0])
        mssql_views = sorted(mssql_views)
        log.info("All views of the \"%s\" in SQL Server:%s", mssql_database, mssql_views)
        # --------------------------------------------------------
        mysql_views = []
        # log.info("Execute \"show table status where comment='view';\" to get all views from the \"" + mysql_database + "\" in MySQL.")
        mysql_sql1 = "show table status where comment='view';"
        mysql_cursor.execute(mysql_sql1)
        mysql_all_views = mysql_cursor.fetchall()

        for i in range(len(mysql_all_views)):
            # print(mysql_all_tables[i][0].lower())
            mysql_views.append(mysql_all_views[i][0])
        # print(mysql_views)
        mysql_views = sorted(mysql_views)
        # print(mysql_views)
        log.info("All views of the \"%s\" in MySQL:%s", mysql_database, mysql_views)

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

    # table field
    test_table_field(mssql_cursor, mysql_cursor, test_tables)

    # table field type and length
    test_table_field_typeAndLength(mssql_cursor, mysql_cursor, test_tables)

    # table field not null
    test_table_field_empty(mssql_cursor, mysql_cursor, test_tables)

    # table field default
    test_table_field_default(mssql_cursor, mysql_cursor, test_tables)

    # table index
    test_table_index(mssql_cursor, mysql_cursor, test_tables)

    # table primary key
    test_table_pri(mssql_cursor, mysql_cursor, test_tables)

    # table automation field
    test_table_autofield(mssql_cursor, mysql_cursor, test_tables)

    # table data
    test_table_data(mssql_cursor, mysql_cursor, test_tables)

    # view field
    test_view_field(mssql_cursor, mysql_cursor, test_views)

    # view field type and length
    test_view_field_typeAndLength(mssql_cursor, mysql_cursor, test_views)

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



all_test()
# if __name__ == "__main__":
#     all_test()