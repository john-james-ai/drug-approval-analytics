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
# Modified : Thursday, August 5th 2021, 3:53:50 am                            #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
# %%
import os
import pytest
import pandas as pd

from src.platform.data.admin import DBAdmin, TableAdmin
from src.platform.config import aact_credentials, dba_credentials
from src.platform.config import test_credentials
from src.platform.base import PGConnector, SAConnector
from tests.test_utils.print import start, end
from tests import announce
# --------------------------------------------------------------------------- #
#                               HELPERS                                       #
# --------------------------------------------------------------------------- #


def get_sa_connection(credentials):
    SAConnector.initialize(credentials.credentials)
    connection = SAConnector.get_connection()
    return connection


def return_sa_connection(connection):
    SAConnector.return_connection(connection)


def get_connection(credentials):
    """Returns a connection to the database and an admin object"""
    PGConnector.initialize(credentials.credentials)
    connection = PGConnector.get_connection()
    return connection


def return_connection(connection):
    PGConnector.return_connection(connection)


def closeall(credentials):
    """Closes all connections and terminates processes on the database."""
    PGConnector.initialize(credentials.credentials)
    PGConnector.close_all_connections()


@pytest.mark.database_admin
class DBAdminTests:

    def __init__(self, testdb: str,  aactdb: str,
                 filepath: str) -> None:
        """Sets up dba,  database names, and cleans up."""
        start(self)
        self._testdb = testdb
        self._aactdb = aactdb  # To test backup restore
        self._filepath = filepath  # Backup/restore filepath
        self._admin = DBAdmin()

        self._reset()

    def _reset(self):

        # Drop all connections
        closeall(dba_credentials)
        closeall(test_credentials)
        closeall(aact_credentials)

        # Drop all test databaases
        connection = get_connection(dba_credentials)
        self._admin.drop(connection=connection, name=self._testdb)

        # Get rid of old backup files
        if os.path.exists(self._filepath):
            os.remove(self._filepath)

    @announce
    def test_create_database(self):
        # Confirm database does not exist.
        connection = get_connection(dba_credentials)
        response = self._admin.exists(
            connection=connection, name=self._testdb)
        return_connection(connection)

        assert not response, "Database Exists Error: Expected False"

        # Create the database
        connection = get_connection(dba_credentials)
        response = self._admin.create(
            connection=connection, name=self._testdb)
        return_connection(connection)

        # Confirm it exists.
        connection = get_connection(dba_credentials)
        response = self._admin.exists(
            connection=connection, name=self._testdb)
        return_connection(connection)

        assert response, "Database Exists Error: Expected True"

    @announce
    def test_drop_database(self):
        # Confirm database does exist.
        connection = get_connection(dba_credentials)
        response = self._admin.exists(
            connection=connection, name=self._testdb)
        return_connection(connection)
        assert response, "Database Exists Error: Expected True"

        # Drop the database
        connection = get_connection(dba_credentials)
        response = self._admin.drop(
            connection=connection, name=self._testdb)
        return_connection(connection)

        # Confirm database does not exists.
        connection = get_connection(dba_credentials)
        response = self._admin.exists(
            connection=connection, name=self._testdb)
        return_connection(connection)
        assert not response, "Database Exists Error: Expected False"

    @announce
    def test_backup(self):
        self._admin.backup(dba_credentials.credentials,
                           self._testdb, self._filepath)
        assert os.path.exists(
            self._filepath), "BackupError: No backup file exists"

    @announce
    def test_restore(self):
        self._admin.restore(dba_credentials.credentials, self._filepath)
        end(self)


class TableTests:

    def __init__(self, testdb: str,  filepath: str) -> None:
        start(self)
        self._testdb = testdb
        self._filepath = filepath
        self._table = 'source'
        self._admin = TableAdmin()
        self._reset()

    def _reset(self):
        # Delete source table
        connection = get_sa_connection(dba_credentials)
        self._admin.drop(
            connection=connection, name=self._table)
        return_sa_connection(connection)

    @announce
    def test_create_table(self):
        # Confirm table does not exist.
        connection = get_connection(dba_credentials)
        response = self._admin.exists(
            connection=connection, name=self._testdb)
        return_connection(connection)
        assert not response, "Table Exists Error: Expected False"

        # Create the table
        df = pd.read_csv(self._filepath)
        connection = get_sa_connection(dba_credentials)
        self._admin.create(
            connection=connection, name=self._table, data=df)
        return_sa_connection(connection)

        # Confirm it exists.
        connection = get_connection(dba_credentials)
        response = self._admin.exists(
            connection=connection, name=self._table)
        return_connection(connection)
        assert response, "Table Exists Error: Expected True"

    @announce
    def test_drop_table(self):
        # Confirm database does exist.
        connection = get_connection(dba_credentials)
        response = self._admin.exists(
            connection=connection, name=self._table)
        return_connection(connection)
        assert response, "Table Exists Error: Expected True"

        # Drop the table
        connection = get_sa_connection(dba_credentials)
        self._admin.drop(
            connection=connection, name=self._table)
        return_sa_connection(connection)

        # Confirm database does not exists.
        connection = get_connection(dba_credentials)
        response = self._admin.exists(
            connection=connection, name=self._table)
        return_connection(connection)
        assert not response, "Table Exists Error: Expected False"

    def test_add_column(self):
        # Recreate the table
        df = pd.read_csv(self._filepath)
        connection = get_sa_connection(dba_credentials)
        self._admin.create(
            connection=connection, name=self._table, data=df)
        return_sa_connection(connection)

        # Confirm column does not exist.
        connection = get_connection(dba_credentials)
        response = self._admin.column_exists(
            connection=connection,
            name=self._table,
            column='foo')
        return_connection(connection)
        assert not response, "Column Exists Error: Expected False"

        # Create the column
        connection = get_connection(dba_credentials)
        response = self._admin.add_column(connection=connection,
                                          name=self._table, column='foo',
                                          datatype='INTEGER')
        return_connection(connection)

        # Confirm it exists.
        connection = get_connection(dba_credentials)
        response = self._admin.column_exists(
            connection=connection, name=self._table)
        return_connection(connection)
        assert response, "Table Exists Error: Expected True"

    def test_drop_column(self):
        # Confirm column exists
        connection = get_connection(dba_credentials)
        response = self._admin.column_exists(
            connection=connection, name=self._table, column='foo')
        return_connection(connection)
        assert response, "Column Exists Error: Expected True"

        # Create the column
        connection = get_connection(dba_credentials)
        response = self._admin.drop_column(connection=connection,
                                           name=self._table, column='foo')
        return_connection(connection)

        # Confirm it does not .
        connection = get_connection(dba_credentials)
        response = self._admin.exists(
            connection=connection, name=self._table)
        return_connection(connection)
        assert not response, "Table Exists Error: Expected False"
        end(self)


def main():
    """Database Unit and Integration Testing """
    filepath = "./tests/data/backup.dmp"
    testdb = 'testdb'
    aactdb = "AACT"

    t = DBAdminTests(testdb, aactdb, filepath)
    t.test_create_database()
    t.test_drop_database()
    # t.test_backup()
    # t.test_restore()

    filepath = "./data/metadata/datasources.csv"
    t = TableTests(testdb, filepath)
    t.test_create_table()
    t.test_drop_table()
    t.test_add_column()
    t.test_drop_column()


if __name__ == '__main__':
    main()
# %%
