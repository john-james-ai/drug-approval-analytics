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
# Modified : Friday, July 30th 2021, 4:19:01 pm                               #
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
from src.operations.sqlgen import CreateArtifactTable
from tests.test_utils.print import start, end
from tests import announce
# --------------------------------------------------------------------------- #
operations_tables = ['artifact', 'parameter', 'event', 'pipeline']
# --------------------------------------------------------------------------- #


@pytest.mark.users
class UserAdminTests:

    @announce
    def cleanup(self, credentials):

        # Delete all tables in postgres
        self._dba = DBAdmin(dba_credentials)
        self._dba.drop('test')
        self._dba.drop(credentials['dbname'])

        # Drop test user
        self._useradmin = UserAdmin(dba_credentials)
        self._useradmin.drop(credentials['user'])

    def __init__(self, credentials):
        start(self)
        self._credentials = credentials
        self.cleanup(credentials)

    @announce
    def test_create(self, credentials):
        self._useradmin.create(credentials, True)
        assert self._useradmin.exists(
            credentials['user']), "User not created."

    @announce
    def test_drop(self, name):
        self._useradmin.drop(name)
        assert not self._useradmin.exists(name), "User not dropped."

    @announce
    def test_wrapup(self, name):
        self._dba.drop(self._credentials['dbname'])
        self._useradmin.drop(name)
        end(self)


@ pytest.mark.database
class DBAdminTests:

    @announce
    def __init__(self, credentials):
        start(self)
        self._ua = UserAdmin(dba_credentials)
        self._ua.create(credentials, create_db=True)
        self._dba = DBAdmin(dba_credentials)
        if self._dba.exists(credentials['dbname']):
            self._dba.drop(credentials['dbname'])

    @announce
    def test_create(self, name):
        self._dba.create(name)
        assert self._dba.exists(name), "Database not created."

    @announce
    def test_grant(self, dbname, username):
        self._dba.grant(dbname, username)

    @announce
    def test_drop(self, name):
        self._dba.drop(name)
        assert not self._dba.exists(name), "Database not dropped."

    @announce
    def test_wrapup(self, name):
        self._dba.drop(name)
        end(self)


class TableAdminTests:

    @announce
    def __init__(self, credentials):
        start(self)
        self._dba = DBAdmin(dba_credentials)
        self._ta = TableAdmin(dba_credentials)
        self._table = 'artifact'

    @announce
    def test_create(self):
        query = CreateArtifactTable()
        command = query.build()
        self._ta.create("artifact", command)
        assert self._ta.exists(
            self._table), "Table {} was not created.".format(
                self._table)

    @announce
    def test_drop_table(self):
        self._ta.drop(self._table, cascade=True)
        assert not self._ta.exists(
            self._table), "Table {} was not dropped.".format(
                self._table)
        end(self)


class BackupRestoreTests:

    @announce
    def test_backup(self, credentials, filepath):
        self._dba.backup(credentials, filepath)
        assert os.path.exists(filepath), "Backup didn't work"

    @announce
    def test_restore(self, credentials, filepath):
        self._dba.restore(credentials, filepath)
        assert self._dba.exists(
            credentials['dbname']), "Database was not restored."


def main():
    dbname = test_credentials['dbname']
    user = test_credentials['user']
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
    t.test_drop_table()
    # t.test_backup(test_credentials, filepath)
    # t.test_drop(dbname)
    # t.test_restore(dbname, test_credentials, filepath)


if __name__ == '__main__':
    main()
# %%
