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
# Modified : Tuesday, July 20th 2021, 12:26:00 am                              #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#
# %%
import pytest
import pandas as pd
import psycopg2
from psycopg2 import connect, sql
import logging

from src.utils.config import DBConfig
from src.data.database.admin import DBAdmin
from src.data.database.sqlgen import CreateDatabase, CreateTable, TableExists
from src.data.database.sqlgen import DropTable
# -----------------------------------------------------------------------------#
logging.basicConfig()
logging.root.setLevel(logging.NOTSET)
logger = logging.getLogger(__name__)


@pytest.mark.querygen
class QueryGenTests:

    def __init__(self):
        DBAdmin().create_database('test')
        config = DBConfig()
        credentials = config('test')
        self._conn = connect(**credentials)

    def test_create_table(self):
        tablename = 'some_tbl'
        sql_command = (CreateTable()
                       .set_name(tablename)
                       .add_column('col1', 'VARCHAR(10)', False, True)
                       .add_column('col2', 'VARCHAR(20)', True, False)
                       .add_column('col3', 'VARCHAR(30)', True, False)
                       .add_column('col4', 'VARCHAR(40)', True, False)
                       .get())

        cur = self._conn.cursor()
        cur.execute(sql_command)
        self._conn.commit()
        cur.close()

        sql_command = TableExists(tablename).get()
        cur = self._conn.cursor()
        cur.execute(sql_command, (tablename,))

        assert cur.fetchone()[0], "Error: Table does not exist."
        logger.info("QueryGenTests CreateTable Passed Test")

    def test_drop_table(self):
        tablename = 'some_tbl'
        sql_command = (DropTable()
                       .set_name(tablename)
                       .get())

        cur = self._conn.cursor()
        cur.execute(sql_command)
        self._conn.commit()
        cur.close()

        sql_command = TableExists(tablename).get()
        cur = self._conn.cursor()
        cur.execute(sql_command, (tablename,))

        assert not cur.fetchone()[0], "Error: Table was not dropped."
        logger.info("QueryGenTests DropTable Passed Test")


def main():
    qgt = QueryGenTests()
    qgt.test_create_table()
    qgt.test_drop_table()


if __name__ == "__main__":
    main()
# %%
