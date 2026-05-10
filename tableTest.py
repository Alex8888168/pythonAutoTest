# _*_ coding: utf-8 _*_
# @Time     : 10/10/2022 7:12 PM
# @Author   : Alex Xu
# @Email    : Alexxu@apjcorp.com
# @File     : tableTest.py
# @Software : PyCharm
from common.duplicate_removal import dup_rem
from common.handle_logging import log, log_file_path
from common.char_type_mapping import CharTypeMapping
from DataMigration.common.connect_mssql import connectMssql
from DataMigration.common.connect_mysql import connectMysql
import hashlib
import time, random
from module.table_autoField import test_table_autofield
from module.table_data import test_table_data
from module.table_field import test_table_field
from module.table_fieldDefault import test_table_field_default
from module.table_fieldEmpty import test_table_field_empty
from module.table_fieldTypeLen import test_table_field_typeAndLength
from module.table_index import test_table_index
from module.table_pri import test_table_pri

# Define global variables

# 'test_tables' Stores the tables that have been migrated successfully
global test_tables

def table_test():
    # 'test_tables' Stores the tables that have been migrated successfully
    global test_tables
    sleep_time = random.randint(0, 5)
    time.sleep(sleep_time)
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
        # log.info("Define an empty array of \"mssql_tables\" to store all tables for a specified SQL Server database.")
        mssql_tables = []
        mssql_tables_global = []
        # log.info("Execute \"select * from sys.tables;\" to get all tables from the \"" + mssql_database + "\" in SQL Server.")
        mssql_sql1 = "select * from sys.tables;"
        mssql_cursor.execute(mssql_sql1)
        mssql_all_tables = mssql_cursor.fetchall()
        for i in range(len(mssql_all_tables)):
            # print(mssql_all_tables[i][0].lower())
            mssql_tables_global.append(mssql_all_tables[i][0])
            mssql_tables.append(mssql_all_tables[i][0])
            # mssql_tables.append(mssql_all_tables[i][0].lower())
        mssql_tables = sorted(mssql_tables)
        mssql_tables_global = sorted(mssql_tables_global)
        # print(mssql_tables)
        # log.info("All tables of the \"" + mssql_database + "\" in SQL Server:%s", mssql_tables_global)
        # --------------------------------------------------------
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

    # # table data
    test_table_data(mssql_cursor, mysql_cursor, test_tables)

    mssql_conn.close()
    mysql_conn.close()
    dup_rem(log_file_path)

table_test()
# if __name__ == "__main__":
#     table_test()