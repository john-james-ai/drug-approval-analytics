#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# =========================================================================== #
# Project  : Drug Approval Analytics                                          #
# Version  : 0.1.0                                                            #
# File     : \tests\test_platform\test_admin.py                               #
# Language : Python 3.9.5                                                     #
# --------------------------------------------------------------------------  #
# Author   : John James                                                       #
# Company  : nov8.ai                                                          #
# Email    : john.james@nov8.ai                                               #
# URL      : https://github.com/john-james-sf/drug-approval-analytics         #
# --------------------------------------------------------------------------  #
# Created  : Tuesday, August 10th 2021, 1:35:36 am                            #
# Modified : Friday, August 13th 2021, 6:20:09 am                             #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
import pytest
import logging
import pandas as pd

from src.platform.database.admin import DBAdmin, UserAdmin, TableAdmin
from src.platform.database.access import PGDao
from src.platform.database.connect import PGConnectionFactory
from src.platform.config import test_credentials, dba_credentials
from src.platform.config import test_pg_credentials
from tests.test_utils.print import start, end
from tests.test_utils.debugging import announce
logger = logging.getLogger(__name__)
# --------------------------------------------------------------------------- #
tables = ["datasource", "dataset", "feature", "countstats", "score",
          "prediction", "datasourceevent", "parameter", "featuretransform",
          "trainingevent", "model"]


@pytest.mark.useradmin
class UserAdminTests:

    @announce
    def test_setup(self, dba_connection):
        connection = dba_connection
        admin = DBAdmin()
        admin.delete("test", connection)

        admin = UserAdmin()
        if admin.exists(test_pg_credentials.user, connection):
            admin.revoke(test_pg_credentials.user,
                         dba_credentials.dbname, connection)
        admin.delete(test_pg_credentials.user, connection)
        if admin.exists(test_credentials.user, connection):
            admin.revoke(test_credentials.user,
                         dba_credentials.dbname, connection)
        admin.delete(test_credentials.user, connection)

    @announce
    @pytest.mark.useradmin
    def test_create(self, dba_connection):
        connection = dba_connection
        admin = UserAdmin()
        admin.create(test_pg_credentials.user,
                     test_pg_credentials.password, connection)
        assert admin.exists(
            test_pg_credentials.user, connection), "UserAdmin ValueError: User does not exist"

    @announce
    @pytest.mark.useradmin
    def test_grant(self, dba_connection, test_pg_connection):
        connection = dba_connection
        useradmin = UserAdmin()
        useradmin.grant(test_pg_credentials.user,
                        dba_credentials.dbname, connection)

        # This should succeed
        connection = test_pg_connection
        dbadmin = DBAdmin()
        dbadmin.create("test", connection)
        assert dbadmin.exists(test_pg_credentials.dbname, connection), """GrantUserError: Was
        not able to create database"""

    @announce
    @pytest.mark.useradmin
    def test_user_connection(self, dba_connection, test_connection):

        # Should be able to connect to test
        connection = test_connection
        dbadmin = DBAdmin()
        assert dbadmin.exists(test_credentials.dbname, connection), """GrantUserError:
        Not able to confirm existence of test db"""


@pytest.mark.dbadmin
class DBAdminTests:

    @announce
    def test_exists(self, dba_connection):
        start(self)
        dbname = "test"
        # First confirm database exists
        connection = dba_connection
        admin = DBAdmin()
        response = admin.exists(name=dbname, connection=connection)
        assert response, "Database Exists Error: Expected True"

    @announce
    def test_delete(self, test_pg_connection):
        # Create database and confirm existence
        connection = test_pg_connection
        dbname = "test"
        admin = DBAdmin()
        admin.delete(name=dbname, connection=connection)
        response = admin.exists(name=dbname, connection=connection)
        assert not response, "Database Exists Error: Expected False"

    @announce
    def test_create(self, test_pg_connection):
        connection = test_pg_connection
        dbname = "test"
        # Create database and confirm existence
        admin = DBAdmin()
        admin.create(name=dbname, connection=connection)
        response = admin.exists(name=dbname, connection=connection)
        assert response, "Database Exists Error: Expected True"


class TableAdminTests:

    @announce
    # def test_setup(self, test_pg_connection):
    #     connection = test_pg_connection
    #     admin = DBAdmin()
    #     # Confirm tables are deleted
    #     for table in tables:
    #         admin.delete(table, connection=connection)
    #         connection.commit()
    #         alive = admin.exists(
    #             table, connection)
    #         assert not alive, "TableError. Expected False Existence"
    #     admin.delete("test", connection)
    #     assert not admin.exists("test", connection), "Test should be gone"
    @announce
    def test_delete(self, dba_connection):
        connection = dba_credentials
        admin = TableAdmin()
        # Confirm table does exist
        for table in tables:
            admin.delete(table, connection=connection)
            connection.commit()
            alive = admin.exists(table, connection)
            assert not alive, "TableError. Expected False Existence"

    @announce
    def test_tables(self, test_pg_connection):
        connection = test_pg_connection

        # Create metadata tables
        filepath = "src/platform/metadata/metadata_table_create.sql"
        tadmin = TableAdmin()
        tadmin.create(filepath=filepath, connection=connection)

        # Confirm table does exist
        for table in tables:
            response = tadmin.exists(table, connection=connection)
            assert not response, "TableError. Expected True Existence"

        # Get list of tables
        tablelist = tadmin.tables(
            test_pg_credentials.dbname, connection=connection)
        assert len(tablelist) == 11, print(tablelist)

    @ announce
    def test_create(self, test_pg_connection):
        # Confirm table does not exist
        connection = test_pg_connection
        admin = DBAdmin()
        for table in tables:
            response = admin.exists(table, connection)
            assert not response, print(
                "TableError. Expected False Existence", response)

        # Create table and confirm existence
        filepath = "src/platform/metadata/metadata_table_create.sql"
        admin.create(filepath=filepath, connection=connection)
        for table in tables:
            response = admin.exists(table, connection)
            assert response, print(
                "TableError. Expected True Existence", response)

    @ announce
    def test_delete(self, test_connection):
        # Confirm table does exist
        connection = test_connection
        admin = DBAdmin()
        for table in tables:
            response = admin.exists(table, connection)
            assert response, print(
                "TableError. Expected True Existence", response)

        # Delete all tables and confirm existence
        filepath = "src/platform/metadata/metadata_table_drop.sql"
        admin.batch_delete(filepath=filepath, connection=connection)
        for table in tables:
            response = admin.exists(table, connection)
            assert not response, print(
                "TableError. Expected False Existence", response)

    @ announce
    def test_load(self, test_connection):
        # Ensure the table doesn't exist
        connection = test_connection
        admin = DBAdmin()

        response = admin.exists("datasource")
        assert not response,\
            print("TestLoadTableError: datasource should not exist.", response)

        # Recreate the table
        filepath = "./data/metadata/datasources.xlsx"
        df = pd.read_excel(filepath, index_col=0)
        admin.load(name="datasource", data=df, connection=connection)

        df.reset_index(inplace=True)
        assert df.shape[0] == 10, print(df, df.columns)
        assert df.shape[1] == 17, print(df, df.columns)

        # Get an access object to read the table

        access = PGDao()
        df2 = access.read(table="datasource", connection=connection)
        assert isinstance(
            df2, pd.DataFrame), "DAOError: Get didn't return a dataframe"
        assert df2.shape[0] == 10, print(df2, df2.columns)
        assert df2.shape[1] == 17, print(df2, df2.columns)

    @ announce
    def test_column_exists(self, test_connection):
        connection = test_connection
        admin = DBAdmin()
        response = admin.column_exists(
            name='datasource', column='lifecycle', connection=connection)
        assert response, "Table error. Column exists failure"

    @ announce
    def test_get_columns(self, test_connection):
        connection = test_connection
        admin = DBAdmin()
        response = admin.get_columns(
            name='datasource', connection=connection)
        assert len(response) == 17, print(response)

    @ announce
    def test_tables_teardown(self, tear_down):
        tear_down
        end(self)
