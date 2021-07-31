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
# Modified : Saturday, July 31st 2021, 3:31:59 am                             #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
# %%
import os
import pytest

from src.operations.database import DBAdmin
from src.operations.config import dba_credentials, test_credentials
from src.operations.sqlgen import CreateArtifactTable
from tests.test_utils.print import start, end
from tests import announce
# --------------------------------------------------------------------------- #
operations_tables = ['artifact', 'parameter', 'event', 'pipeline']
# --------------------------------------------------------------------------- #


@ pytest.mark.database_build
class BuildTests:

    def __init__(self, dba_credentials, test_credentials, masterdb, testdb):
        """Sets up dba, user credentials, database names, and cleans up."""
        start(self)
        self.dba_credentials = dba_credentials
        self._user_credentials = test_credentials
        self._masterdb = masterdb
        self._testdb = testdb
        self._table = "artifact"
        self.dba = DBAdmin(dba_credentials)
        self._cleanup()

    @announce
    def _cleanup(self):
        """Removes all table, user, database, artifacts """
        self.dba.revoke_database_privileges(self._masterdb,
                                            self._user_credentials['user'])
        if self.dba.table_exists(self._table):
            self.dba.drop_table(self._table)

        if self.dba.database_exists(self._testdb):
            self.dba.drop_database(self._testdb)

        if self.dba.user_exists(self._user_credentials['user']):
            self.dba.drop_user(self._user_credentials['user'])

    @announce
    def test_create_user(self):
        """Creates the user that will be performing the activities."""
        self.dba.create_user(self._user_credentials, create_db=True)
        assert self.dba.user_exists(
            self._user_credentials['user']), "User not created."

    @announce
    def test_grant_user_privileges(self):
        """Grants user all privileges on master db."""
        self.dba.grant_database_privileges(
            self._masterdb, self._user_credentials['user'])

    @announce
    def test_create_database(self):
        """Create database under user credentials."""
        self.dba = DBAdmin(self._user_credentials)
        self.dba.create_database(self._testdb)
        assert self.dba.database_exists(self._testdb), "Database not created."

    @announce
    def test_create_table(self):
        """User creates table."""
        query = CreateArtifactTable()
        command = query.build()
        self.dba.create_table(self._table, command)
        assert self.dba.table_exists(
            self._table), "Table {} was not created.".format(
                self._table)
        end(self)


@ pytest.mark.database_backup_restore
class BackupRestoreTests:

    def __init__(self, dba, credentials, filepath):
        start(self)
        self.dba = dba
        self._credentials = credentials
        self._filepath = filepath

    @announce
    def test_backup(self):
        self.dba.backup_database(self._credentials, self._filepath)
        assert os.path.exists(self._filepath), "Backup didn't work"

    @announce
    def test_restore(self):
        self.dba.restore_database(self._credentials, self._filepath)
        assert self.dba.exists(
            self._credentials['dbname']), "Database was not restored."
        end(self)


@pytest.mark.database
class TearDownTests:

    def __init__(self, dba_credentials, user_credentials, dbname):
        start(self)
        self._table = "artifact"
        self._dbname = dbname
        self._dba_credentials = dba_credentials
        self._user_credentials = user_credentials
        self.dba = DBAdmin(dba_credentials)

    @announce
    def test_drop_table(self):
        self.dba.drop_table(self._table, cascade=True)
        assert not self.dba.table_exists(
            self._table), "Table {} was not dropped.".format(
                self._table)

    @announce
    def test_drop_database(self):
        self.dba.drop_database(self._dbname)
        assert not self.dba.exists(self._dbname), "Database not dropped."

    @announce
    def test_drop_user(self):
        self.dba.drop_user(self._user_credentials['user'])
        assert not self.dba.user_exists(
            self._user_credentials['user']), "User not dropped."
        end(self)


def test_build(dba_credentials, user_credentials, masterdb, testdb):

    t = BuildTests(
        dba_credentials, user_credentials, masterdb, testdb)
    t.test_create_user()
    t.test_grant_user_privileges()
    t.test_create_database()
    t.test_create_table()
    return t.dba


def test_backup_restore(dba, credentials, filepath):
    t = BackupRestoreTests(dba, credentials, filepath)
    t.test_backup()
    t.test_restore()
    return t.dba


def test_teardown(dba_credentials, user_credentials, dbname):
    t = TearDownTests(dba_credentials, user_credentials, dbname)
    t.test_drop_table()
    t.test_drop_database()
    t.test_drop_user()


def main():
    """Database Unit and Integration Testing """
    filepath = "./tests/data/backup.dmp"
    user_credentials = test_credentials
    masterdb = 'postgres'
    testdb = 'testdb'

    # Build tests
    dba = test_build(dba_credentials, user_credentials, masterdb, testdb)
    # Backup restore tests
    dba = test_backup_restore(dba, user_credentials, filepath)
    # Teardown tests
    test_teardown(dba)


if __name__ == '__main__':
    main()
# %%
