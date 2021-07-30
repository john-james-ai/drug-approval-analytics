#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# =========================================================================== #
# Project  : Drug Approval Analytics                                          #
# Version  : 0.1.0                                                            #
# File     : \tests\test_operations\test_setup.py                             #
# Language : Python 3.9.5                                                     #
# --------------------------------------------------------------------------  #
# Author   : John James                                                       #
# Company  : nov8.ai                                                          #
# Email    : john.james@nov8.ai                                               #
# URL      : https://github.com/john-james-sf/drug-approval-analytics         #
# --------------------------------------------------------------------------  #
# Created  : Wednesday, July 28th 2021, 8:57:51 pm                            #
# Modified : Thursday, July 29th 2021, 3:10:17 pm                             #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
# %%
import os
import pytest
from src.operations.database import UserAdmin, DBAdmin, TableAdmin
from src.operations.config import test_credentials, dba_credentials
from src.operations.sqllib import operations_tables
from tests.test_utils.print import start, end

# --------------------------------------------------------------------------- #


@pytest.mark.users
class UserAdminTests:

    def cleanup(self, credentials):

        # Delete all tables in postgres
        self._dba = DBAdmin(dba_credentials)
        self._ta = TableAdmin(dba_credentials)
        for name, command in operations_tables.items():
            self._ta.drop(name)

        # Delete all tables in testdb
        if self._dba.exists(credentials.dbname):
            self._ta = TableAdmin(credentials)
            for name, command in operations_tables.items():
                self._ta.drop(name)

        # Drop test user
        self._useradmin = UserAdmin(dba_credentials)
        self._useradmin.drop(credentials.user)

        # Drop test database
        self._dba = DBAdmin(dba_credentials)
        self._dba.drop(credentials.dbname)

    def __init__(self, credentials):
        start(self)
        self._credentials = credentials
        self.cleanup(credentials)

    def test_create(self, credentials):
        self._useradmin.create(credentials, True)
        assert self._useradmin.exists(
            credentials.user), "User not created."

    def test_drop(self, name):
        self._useradmin.drop(name)
        assert not self._useradmin.exists(name), "User not dropped."

    def test_wrapup(self, name):
        self._dba.drop(self._credentials.dbname)
        self._useradmin.drop(name)
        end(self)


@ pytest.mark.database
class DBAdminTests:

    def __init__(self, credentials):
        start(self)
        self._ua = UserAdmin(dba_credentials)
        self._ua.create(credentials, create_db=True)
        self._dba = DBAdmin(credentials)
        if self._dba.exists(credentials.dbname):
            self._dba.drop(credentials.dbname)

    def test_create(self, name):
        self._dba.create(name)
        assert self._dba.exists(name), "Database not created."

    def test_grant(self, dbname, username):
        self._dba.grant(dbname, username)

    def test_drop(self, name):
        self._dba.drop(name)
        assert not self._dba.exists(name), "Database not dropped."

    def test_wrapup(self, name):
        self._dba.drop(name)
        end(self)


class TableAdminTests:

    def __init__(self, credentials):
        start(self)
        self._dba = DBAdmin(credentials)
        self._ta = TableAdmin(credentials)

    def test_create(self):
        for name, command in operations_tables.items():
            print(*command)
            self._ta.create(name, *command)
            assert self._ta.exists(
                name), "Table {} was not created.".format(name)

    def test_add_columns(self):
        columns = {'first': 'INTEGER',
                   'second': 'VARCHAR(24)', 'third': 'bool'}
        for name, command in operations_tables.items():
            self._ta.add_columns(name, columns)
            for c in columns:
                assert self._ta.column_exists(name, c), \
                    "Column {} not added".format(c)

    def test_drop_columns(self):
        columns = ['first', 'second', 'third']
        for name, command in operations_tables.items():
            self._ta.drop_columns(name, columns)
            for c in columns:
                assert not self._ta.column_exists(name, c), \
                    "Column {} not dropped".format(c)

    def test_drop_table(self):
        for name, command in operations_tables.items():
            self._ta.drop(name)
            assert not self._ta.exists(
                name), "Table {} was not dropped.".format(name)
        end(self)


class BackupRestoreTests:

    def test_backup(self, credentials, filepath):
        self._dba.backup(credentials, filepath)
        assert os.path.exists(filepath), "Backup didn't work"

    def test_restore(self, credentials, filepath):
        self._dba.restore(credentials, filepath)
        assert self._dba.exists(
            credentials.dbname), "Database was not restored."


def main():
    dbname = 'test'
    user = 'testuser'
    # filepath = "./test_backup.dmp"
    t = UserAdminTests(test_credentials)
    t.test_create(test_credentials)
    t.test_drop(user)
    t.test_wrapup(user)

    t = DBAdminTests(test_credentials)
    t.test_create(dbname)
    t.test_grant(dbname, user)

    t = TableAdminTests(test_credentials)
    t.test_create()
    t.test_add_columns()
    t.test_drop_columns()
    t.test_drop_table()
    # t.test_backup(test_credentials, filepath)
    # t.test_drop(dbname)
    # t.test_restore(dbname, test_credentials, filepath)


if __name__ == '__main__':
    main()
# %%
