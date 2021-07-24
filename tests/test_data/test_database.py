#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# =========================================================================== #
# Project  : Drug Approval Analytics                                          #
# Version  : 0.1.0                                                            #
# File     : \tests\test_data\test_database.py                                #
# Language : Python 3.9.5                                                     #
# --------------------------------------------------------------------------  #
# Author   : John James                                                       #
# Company  : nov8.ai                                                          #
# Email    : john.james@nov8.ai                                               #
# URL      : https://github.com/john-james-sf/drug-approval-analytics         #
# --------------------------------------------------------------------------  #
# Created  : Sunday, July 4th 2021, 6:46:35 pm                                #
# Modified : Saturday, July 24th 2021, 5:13:56 am                             #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
# %%
import os
import time
import pytest
import pandas as pd
from src.platform.database import DBA, TableAdmin
from src.utils.config import dba_credentials
print(dba_credentials)
# -----------------------------------------------------------------------------#


@pytest.mark.dbadmin
class DBATests1:

    def __init__(self):
        dbadmin = DBA(dba_credentials)
        dbadmin.drop('test')

    def test_database_exists(self):
        dbadmin = DBA(dba_credentials)
        result = dbadmin.exists('test')
        assert result is False, "Database Exists Error: Existence \
            evaluated as True."
        print('\n#', 80*"-", '#')
        print(" "*27, "Database Existence Testing Complete!")
        print('#', 80*"-", '#')

    def test_create_database(self):
        dbadmin = DBA(dba_credentials)
        dbadmin.create('test')
        result = dbadmin.exists('test')
        assert result is True, "Database Exists Error: Existence \
            evaluated as False."
        print('\n#', 80*"-", '#')
        print(" "*27, "Database Creation Testing Complete!")
        print('#', 80*"-", '#')

    def test_drop_database(self):
        dbadmin = DBA(dba_credentials)
        dbadmin.drop('test')
        result = dbadmin.exists('test')
        assert result is False, "Database Drop Error: Existence \
            evaluated as returned True."
        print('\n#', 80*"-", '#')
        print(" "*27, "Database Drop Testing Complete!")
        print('#', 80*"-", '#')


class TableTests:

    def __init__(self):
        dba = TableAdmin(dba_credentials)
        name = 'test_table'
        schema = 'test_schema'
        dba.drop(name, schema)

    def _build_schema(self):
        filepath = "./data/metadata/repository.csv"
        df = pd.read_csv(filepath, usecols=['column', 'command'])
        config = df.to_dict()
        columns = {}
        for col, command in config.items():
            columns[col] = command
        return columns
        print('\n#', 80*"-", '#')
        print(" "*27, "Database Schema Built!")
        print('#', 80*"-", '#')

    def test_create_table(self):
        columns = self._build_schema()
        name = 'test_table'
        schema = 'test_schema'
        dba = TableAdmin(dba_credentials)
        dba.create(name=name, schema=schema, columns=columns)
        response = dba.exists(name, schema)
        assert response is True, "Create Table Error: Table does not exist."
        print('\n#', 80*"-", '#')
        print(" "*27, "Table Creation Testing Complete!")
        print('#', 80*"-", '#')

    def test_drop_table(self):
        name = 'test_table'
        schema = 'test_schema'
        dba = TableAdmin(dba_credentials)
        dba.drop(name=name, schema=schema)
        response = dba.exists(name, schema)
        assert response is False, "Drop Table Error: Table wasn't dropped."
        print('\n#', 80*"-", '#')
        print(" "*27, "Table Drop Testing Complete!")
        print('#', 80*"-", '#')

    def test_insert(self):
        pass
        # TODO

    def test_update(self):
        pass
        # TODO

    def test_query(self):
        pass
        # TODO

    def test_delete(self):
        pass
        # TODO


class DBATests2:

    def test_backup_database(self):
        # Confirm the database exists.
        dba = DBA(dba_credentials)
        response = dba.exists('test')
        assert response is True, "Error Test Backup. Database doesn't exist."
        # Perform backup
        filepath = "../data/backup.dmp"
        dba = DBA(dba_credentials)
        dba.backup(name='test', filepath=filepath)
        time.sleep(10)  # Give it 10 seconds
        assert os.path.exists(
            filepath), "Database Backup Error. File does not exist."
        print('\n#', 80*"-", '#')
        print(" "*27, "Database Backup Testing Complete!")
        print('#', 80*"-", '#')

    def test_restore_database(self):
        # Drop the database
        dba = DBA(dba_credentials)
        dba.drop('test')
        response = dba.exists('test')
        assert response is True, "Error Test Restore. Didn't Drop"
        # Perform restore
        filepath = "../data/backup.dmp"
        dba = DBA(dba_credentials)
        dba.restore(name='test', filepath=filepath)
        time.sleep(10)  # Give it 10 seconds
        # Confirm it exists.
        response = dba.exists('test')
        assert response is True, "Error Test Restore. Didn't work."
        print('\n#', 80*"-", '#')
        print(" "*27, "Database Restore Testing Complete!")
        print('#', 80*"-", '#')


def main():
    dba = DBATests1()
    dba.test_database_exists()
    dba.test_create_database()
    dba.test_drop_database()

    dba = TableTests()
    dba.test_create_table()
    # dba.test_drop_table()
    # # dba.test_insert()
    # # dba.test_update()
    # # dba.test_query()
    # # dba.test_delete()

    # dba = DBATests2()
    # dba.test_backup_database()
    # dba.test_restore_database()

    print('\n\n#', 80*"=", '#')
    print(" "*27, "Database Testing Complete!")
    print('#', 80*"=", '#')


if __name__ == "__main__":
    main()
# %%
