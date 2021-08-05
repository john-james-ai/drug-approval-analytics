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
# Modified : Wednesday, August 4th 2021, 9:14:15 pm                           #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
# %%
import os
import pytest

from src.platform.data.admin import DBAdmin, TableAdmin
from src.platform.config import aact_credentials, dba_credentials
from src.platform.config import test_credentials
from src.platform.base import PGConnector
from tests.test_utils.print import start, end
from tests import announce
# --------------------------------------------------------------------------- #
#                               HELPERS                                       #
# --------------------------------------------------------------------------- #


def get_db_admin(credentials):
    """Returns a connection to the database and an admin object"""
    PGConnector.initialize(credentials.credentials)
    connection = PGConnector.get_connection()
    admin = DBAdmin()
    return connection, admin


def get_table_admin(credentials):
    """Returns a connection to the database and a table admin object."""
    PGConnector.initialize(credentials.credentials)
    connection = PGConnector.get_connection()
    admin = TableAdmin()
    return connection, admin


def closeall(credentials):
    """Closes all connections and terminates processes on the database."""
    PGConnector.initialize(credentials.credentials)
    connection = PGConnector.get_connection()
    admin = DBAdmin()
    admin.terminate_database_processes(connection, credentials.dbname)
    PGConnector.return_connection(connection)
    PGConnector.close_all_connections()


def run_query(connection, command):
    cursor = connection.cursor()
    cursor.execute(command)
    return cursor.fetchall()


@ pytest.mark.database_admin
class BuildDBTests:

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

        # Drop all connections
        closeall(dba_credentials)
        closeall(test_credentials)
        closeall(aact_credentials)

        # Drop all test databaases
        connection, admin = get_db_admin(dba_credentials)
        admin = DBAdmin()
        admin.drop_database(connection=connection, name=self._testdb)

        # Get rid of old backup files
        if os.path.exists(self._filepath):
            os.remove(self._filepath)

    @announce
    def test_database_does_not_exist(self):
        # Get Connection and admin object.
        connection, admin = get_db_admin(dba_credentials)
        response = admin.database_exists(
            connection=connection, name=self._testdb)

        assert not response,\
            "Database Exists Error: Expected False. Got {}".format(response)
        closeall(dba_credentials)

    @announce
    def test_create_database(self):
        connection, admin = get_db_admin(dba_credentials)
        admin.create_database(connection, name)(
            connection=connection, name=self._testdb)
        closeall(dba_credentials)

    @announce
    def test_database_does_exist(self):
        connection, admin = get_db_admin(dba_credentials)
        response = admin.database_exists(
            connection=connection, name=self._testdb)
        closeall(dba_credentials)

        assert response, \
            "Database Exists Error: Expected True. Got {}".format(response)
        end(self)


class BuildTableTests:

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

        # Drop all connections
        closeall(dba_credentials)
        closeall(test_credentials)
        closeall(aact_credentials)

        # Drop all test databaases
        connection, admin = get_db_admin(dba_credentials)
        admin = DBAdmin()
        admin.drop_database(connection=connection, name=self._testdb)

        # Get rid of old backup files
        if os.path.exists(self._filepath):
            os.remove(self._filepath)

        # Create clean test database
        connection, admin = get_db_admin(dba_credentials)
        admin = DBAdmin()
        admin.create_database(connection=connection, name=self._testdb)

    @announce
    def test_create_table(self):
        # First grant user privileges on the database
        connection, admin = get_db_admin(dba_credentials)
        admin.grant(connect, test_credentials.user, self._testdb)
        closeall(dba_credentials)

        # Obtain the data from the datasources file
        df = pd.read_csv(self._filepath)
        # Create the table as test user.
        connection, admin = get_db_admin(test_credentials)
        admin.create_table(connection, 'sources', df)
        response = admin.table_exists(connection, 'sources')
        closeall(dba_credentials)
        assert response, \
            "Database Exists Error: Expected True. Got {}".format(response)

    @announce
    def test_add_columns(self):
        columns = [
            {'foo': 'VARCHAR(24)'},
            {'bar': 'INTEGER'}
        ]
        connection, admin = get_db_admin(test_credentials)
        admin.add_columns(connection, 'source', columns)

    def test_drop_table(self):
        connection, admin = get_db_admin(dba_credentials)
        admin.drop_table(connection, 'sources')
        response = admin.table_exists(connection, 'sources')
        closeall(dba_credentials)
        assert not response, \
            "Database Exists Error: Expected False. Got {}".format(response)









  @announce
   def test_drop_database(self):
        PGConnector().initialize(dba_credentials.credentials)
        connection = PGConnector.get_connection()
        admin = DBAdmin()
        admin.drop(connection=connection, name=self._testdb)
        response = admin.exists(
            connection=connection, name=self._testdb)
        PGConnector().return_connection(connection)
        assert not response, "Database Drop Error: Expected False. \
            Got {}".format(response)
        PGConnector.close_all_connections()

    @announce
    def test_copy_database(self):
        PGConnector().initialize(aact_credentials.credentials)
        connection = PGConnector.get_connection()
        admin = DBAdmin()
        admin.copy(connection=connection, source=self._aactdb,
                   target=self._testdb)
        response = admin.exists(connection=connection, name=self._testdb)
        print(response)
        PGConnector().return_connection(connection)
        assert response, "Database Copy Error: Expected True. \
            Got {}".format(response)
        PGConnector.close_all_connections()

    @announce
    def test_backup_database(self):
        admin = DBAdmin()
        admin.backup(credentials=aact_credentials.credentials,
                     filepath=self._filepath)
        assert os.path.exists(self._filepath), "DatabaseBackupError: \
            Backup file does not exist."
        assert os.path.getfilesize(self._filepath) > self._empty_filesize,\
            "DatabaseBackupError: Backup file is empty."

    @announce
    def test_restore_database(self):
        admin = DBAdmin()
        admin.restore(credentials=aact_credentials, filepath=self._filepath)
        # Delete  backup file, backup again, check filesize.
        # If large file, then the restore was successful
        end(self)


def main():
    """Database Unit and Integration Testing """
    filepath = "./tests/data/backup.dmp"
    masterdb = 'postgres'
    testdb = 'testdb'
    aactdb = "AACT"

    t = DBAdminTests(masterdb, testdb, aactdb, filepath)
    # t.test_database_exists()
    # t.test_create_database()
    # t.test_drop_database()
    # t.test_copy_database()
    t.test_backup_database()
    # t.test_restore_database()


if __name__ == '__main__':
    main()
# %%
