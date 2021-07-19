#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#==============================================================================#
# Project  : Drug Approval Analytics                                           #
# Version  : 0.1.0                                                             #
# File     : \tests\test_data\test_sql_gen.py                                  #
# Language : Python 3.9.5                                                      #
# -----------------------------------------------------------------------------#
# Author   : John James                                                        #
# Company  : nov8.ai                                                           #
# Email    : john.james@nov8.ai                                                #
# URL      : https://github.com/john-james-sf/drug-approval-analytics          #
# -----------------------------------------------------------------------------#
# Created  : Monday, July 19th 2021, 4:46:18 pm                                #
# Modified : Monday, July 19th 2021, 5:44:20 pm                                #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#
#%%
import pytest
import pandas as pd
import psycopg2
from psycopg2 import connect, sql

from src.utils.config import DBConfig
from src.data.database.admin import DBAdmin
from src.data.database.querygen import CreateDatabase, CreateTable
# -----------------------------------------------------------------------------#

@pytest.mark.querygen
class QueryGenTests:

    def __init__(self):
        DBAdmin().create_database('test')
        config = DBConfig()
        credentials = config('test')
        self._conn = connect(**credentials)

    def get_table_info(self, tablename):
        q = f'SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = %s;'            

        cur = self._conn.cursor()
        cur.execute(q, (tablename,))  # (table_name,) passed as tuple
        print(cur.fetchall())


    def test_create_table(self):
        tablename = 'some_tbl'
        sql_command = (CreateTable()
        .set_name(tablename)
        .add_column('col1', 'VARCHAR(10)', False, True)
        .add_column('col2', 'VARCHAR(20)', True, False)
        .add_column('col3', 'VARCHAR(30)', True, False)
        .add_column('col4', 'VARCHAR(40)', True, False)
        .gen())

        cur = self._conn.cursor()
        cur.execute(sql_command)  
        self._conn.commit()       
        cur.close()

        self.get_table_info(tablename)


def main():
    qgt = QueryGenTests()
    qgt.test_create_table()
    
if __name__ == "__main__":
    main()   
#%%
