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
# Modified : Tuesday, August 3rd 2021, 7:37:45 pm                             #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
# %%
import os
import pytest

from src.platform.data.admin import DBAdmin
from src.platform.config import aact_credentials, dba_credentials
from src.platform.config import test_credentials
from src.platform.base import PGConnector
from tests.test_utils.print import start, end
from tests import announce
# --------------------------------------------------------------------------- #


@ pytest.mark.database_admin
class DBAdminTests:

    def __init__(self, masterdb: str, testdb: str,  aactdb: str,
                 filepath: str) -> None:
        """Sets up dba,  database names, and cleans up."""
        start(self)
        self._masterdb = masterdb
        self._testdb = testdb
        self._aactdb = aactdb
        self._filepath = filepath

        self._reset()

    def _reset(self):
        # Drop the test database if it exists
        PGConnector.initialize(dba_credentials.credentials)
        connection = PGConnector.get_connection()
        admin = DBAdmin()
        admin.drop(connection=connection, name=self._testdb)
        PGConnector().return_connection()(connection)
        # Get rid of old backup files
        os.path.remove(self._filepath)
        # Note the empty file size. We'll use that to check backup/restore
        self._empty_filesize = os.path.getsize(self._filepath)

    @announce
    def test_database_exists(self):
        PGConnector().initialize(dba_credentials.credentials)
        connection = PGConnector.get_connection()
        admin = DBAdmin()

        response = admin.exists(connection=connection, name=self._testdb)
        PGConnector().return_connection()(connection)
        assert not response, "Database Exists Error: Expected False. \
            Got {}".format(response)

    @announce
    def test_create_database(self):
        PGConnector().initialize(dba_credentials.credentials)
        connection = PGConnector.get_connection()
        admin = DBAdmin()

        admin.create(pgconn=connection, name=self._testdb)
        response = admin.exists(connection=connection, name=self._testdb)
        PGConnector().return_connection()(connection)
        assert response, "Database Create Error: Expected True. \
            Got {}".format(response)

    @announce
    def test_drop_database(self):
        PGConnector().initialize(dba_credentials.credentials)
        connection = PGConnector.get_connection()
        admin = DBAdmin()
        self.db_admin.drop(connection=connection, name=self._testdb)
        response = admin.exists(
            connection=connection, name=self._testdb)
        PGConnector().return_connection()(connection)
        assert not response, "Database Drop Error: Expected False. \
            Got {}".format(response)

    @announce
    def test_copy_database(self):
        PGConnector().initialize(aact_credentials.credentials)
        connection = PGConnector.get_connection()
        admin = DBAdmin()
        admin.copy(connection=connection, source=self._aactdb,
                   target=self._testdb)
        response = admin.exists(connection=connection, name=self._testdb)
        PGConnector().return_connection()(connection)
        assert response, "Database Copy Error: Expected True. \
            Got {}".format(response)

    @announce
    def test_backup_database(self):
        admin = DBAdmin()
        admin.backup(credentials=test_credentials, filepath=self._filepath)
        assert os.path.exists(self._filepath), "DatabaseBackupError: \
            Backup file does not exist."
        assert os.path.getfilesize(self._filepath) > self._empty_filesize,\
            "DatabaseBackupError: Backup file is empty."
        PGConnector().initialize(aact_credentials.credentials)
        connection = PGConnector.get_connection()
        admin = DBAdmin()
        admin.drop(connection=connection, name=self._testdb)
        PGConnector().return_connection()(connection)

    @announce
    def test_restore_database(self):
        connection = PGConnector().initialize(test_credentials.credentials)\
            .get_connection()
        admin = DBAdmin()
        admin.create(connection=connection, name=self._testdb)
        admin.restore(credentials=test_credentials, filepath=self._filepath)
        PGConnector().return_connection()(connection)
        # Delete  backup file, backup again, check filesize.
        # If large file, then the restore was successful
        os.path.remove(self._filepath)
        admin.backup(credentials=test_credentials, filename=self._filepath)
        assert os.path.exists(self._filepath), "DatabaseBackupError: \
            Backup file does not exist."
        assert os.path.getsize(self._filepath) > self._empty_filesize,\
            "DatabaseRestoreError: Backup file for restored database is empty."

        end(self)


def main():
    """Database Unit and Integration Testing """
    filepath = "./tests/data/backup.dmp"
    masterdb = 'postgres'
    testdb = 'testdb'
    aactdb = "AACT"

    t = DBAdminTests(masterdb, testdb, aactdb, filepath)
    t.test_database_exists()
    t.test_create_database()
    t.test_drop_database()
    t.test_copy_database()
    t.test_backup_database()
    t.test_restore_database()


if __name__ == '__main__':
    main()
# %%
