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
# Modified : Wednesday, August 11th 2021, 7:06:43 pm                          #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
import pytest
import pandas as pd

from src.platform.database.admin import DBAdmin, TableAdmin, UserAdmin
from src.platform.database.access import PGDao
from src.platform.config import dba_credentials, test_credentials
from src.platform.config import test_pg_credentials
from tests.test_utils.print import start, end
from tests import announce

# --------------------------------------------------------------------------- #
tables = ["datasource", "dataset", "feature", "countstats", "score",
          "prediction", "datasourceevent", "parameter", "featuretransform",
          "trainingevent", "model"]


@pytest.mark.useradmin
class UserAdminTests:

    @announce
    @pytest.mark.useradmin
    def test_setup(self):
        dbadmin = DBAdmin(dba_credentials)
        if dbadmin.exists("test"):
            dbadmin.delete("test")
        dbadmin.commit()
        dbadmin.close()

        useradmin = UserAdmin(dba_credentials)
        if useradmin.exists(test_credentials.user):
            useradmin.revoke(test_credentials.user, test_pg_credentials.dbname)
            useradmin.delete(test_credentials.user)
        useradmin.commit()
        useradmin.close()

    @announce
    @pytest.mark.useradmin
    def test_create_user(self):
        admin = UserAdmin(dba_credentials)
        if admin.exists(test_credentials.user):
            admin.revoke(test_credentials.user, dba_credentials.dbname)
            admin.delete(test_credentials.user)
            admin.commit()
        admin.create(test_credentials.user, test_credentials.password)
        assert admin.exists(
            test_credentials.user), "UserAdmin ValueError: User does not exist"
        admin.commit()
        admin.close()

    @announce
    @pytest.mark.useradmin
    def test_grant_user(self):
        useradmin = UserAdmin(dba_credentials)
        useradmin.grant(test_credentials.user, dba_credentials.dbname)
        useradmin.commit()
        useradmin.close()

        # This should succeed
        dbadmin = DBAdmin(test_pg_credentials)
        dbadmin.create(test_credentials.dbname)
        assert dbadmin.exists(test_credentials.dbname), """GrantUserError: Was
        not able to create database"""
        dbadmin.commit()
        dbadmin.close()

        # Should be able to connect to test
        dbadmin = DBAdmin(test_credentials)
        assert dbadmin.exists(test_credentials.dbname), """GrantUserError:
        Not able to confirm existence of test db"""
        dbadmin.commit()
        dbadmin.close()

    @announce
    @pytest.mark.useradmin
    def test_user_teardown(self):
        dbadmin = DBAdmin(dba_credentials)
        if dbadmin.exists("test"):
            dbadmin.delete("test")
        dbadmin.commit()
        dbadmin.close()

        useradmin = UserAdmin(dba_credentials)
        if useradmin.exists(test_credentials.user):
            useradmin.revoke(test_credentials.user, test_pg_credentials.dbname)
            useradmin.delete(test_credentials.user)

        end(self)


@ pytest.mark.dbadmin
class DBAdminTests:

    @ announce
    @ pytest.mark.dbadmin
    def test_setup(self):
        dbadmin = DBAdmin(dba_credentials)
        if dbadmin.exists("test"):
            dbadmin.delete("test")

        admin = UserAdmin(dba_credentials)
        if admin.exists(test_credentials.user):
            admin.revoke(test_credentials.user, dba_credentials.dbname)
            admin.delete(test_credentials.user)
            admin.commit()
        admin.create(test_credentials.user, test_credentials.password)
        assert admin.exists(
            test_credentials.user), "UserAdmin ValueError: User does not exist"
        admin.commit()
        admin.close()

    @ announce
    @ pytest.mark.dbadmin
    def test_create_database(self):
        test = 'test'
        # First confirm database doesn't exist
        admin = DBAdmin(test_pg_credentials)
        response = admin.exists(name=test)
        assert not response, "Database Exists Error: Expected False"

        # Create database and confirm existence
        admin.create(name=test)
        response = admin.exists(name=test)
        assert response, "Database Exists Error: Expected True"

    @ announce
    def test_delete_database(self):
        # First confirm database does exist
        test = 'test'
        admin = DBAdmin(test_pg_credentials)
        response = admin.exists(name=test)
        assert response, "Database Exists Error: Expected True"

        # Create database and confirm existence
        admin.delete(name=test)
        response = admin.exists(name=test)
        assert not response, "Database Exists Error: Expected False"

    def test_tables(self):
        dbname = 'test'
        # Create database and confirm existence
        dbadmin = DBAdmin(test_pg_credentials)
        dbadmin.delete(name=dbname)
        dbadmin.commit()
        dbadmin.create(name=dbname)
        dbadmin.commit()
        response = dbadmin.exists(name=dbname)
        dbadmin.close()
        assert response, "Database Exists Error: Expected True"

        # Confirm table does not exist
        admin = TableAdmin(test_credentials)
        for table in tables:
            response = admin.exists(table)
            assert not response, "TableError. Expected False Existence"

        # Create metadata tables
        filepath = "src/platform/metadata/metadata_table_create.sql"
        tadmin = TableAdmin(test_credentials)
        tadmin.create(filepath=filepath)
        tadmin.commit()

        # Confirm table does exist
        admin = TableAdmin(test_credentials)
        for table in tables:
            response = admin.exists(table)
            assert response, "TableError. Expected False Existence"

        # Get list of tables
        dbadmin = DBAdmin(test_credentials)
        tablelist = dbadmin.tables(test_credentials.dbname)
        assert len(tablelist) == 11, print(tablelist)

    def test_database_teardown(self):
        dbadmin = DBAdmin(dba_credentials)
        if dbadmin.exists("test"):
            dbadmin.delete("test")
        dbadmin.commit()

        useradmin = UserAdmin(dba_credentials)
        if useradmin.exists(test_credentials.user):
            useradmin.revoke(test_credentials.user, test_pg_credentials.dbname)
            useradmin.delete(test_credentials.user)

        end(self)


class TableAdminTests:

    def test_setup(self):
        start(self)
        dbadmin = DBAdmin(dba_credentials)
        if dbadmin.exists("test"):
            dbadmin.delete("test")
        dbadmin.commit()
        dbadmin.close()

        admin = UserAdmin(dba_credentials)
        if admin.exists(test_credentials.user):
            admin.revoke(test_credentials.user, dba_credentials.dbname)
            admin.delete(test_credentials.user)
            admin.commit()
        admin.create(test_credentials.user, test_credentials.password)
        assert admin.exists(
            test_credentials.user), "UserAdmin ValueError: User does not exist"

        admin = DBAdmin(test_pg_credentials)
        admin.create(name="test")
        response = admin.exists(name="test")
        assert response, "Database Exists Error: Expected True"

    @ announce
    def test_create_table(self):
        # Confirm table does not exist
        admin = TableAdmin(test_credentials)
        for table in tables:
            response = admin.exists(table)
            assert not response, print(
                "TableError. Expected False Existence", response)

        # Create table and confirm existence
        filepath = "src/platform/metadata/metadata_table_create.sql"
        admin.create(filepath=filepath)
        for table in tables:
            response = admin.exists(table)
            assert response, print(
                "TableError. Expected True Existence", response)

    @ announce
    def test_delete_table(self):
        # Confirm table does exist
        admin = TableAdmin(test_credentials)
        for table in tables:
            response = admin.exists(table)
            assert response, print(
                "TableError. Expected True Existence", response)

        # Delete all table and confirm existence
        filepath = "src/platform/metadata/metadata_table_drop.sql"
        admin.delete(filepath=filepath)
        for table in tables:
            response = admin.exists(table)
            assert not response, print(
                "TableError. Expected False Existence", response)

    @ announce
    def test_load_table(self):
        admin = TableAdmin(test_credentials)
        # Ensure the table doesn't exist
        response = admin.exists("datasource")
        assert not response,\
            print("TestLoadTableError: datasource should not exist.", response)

        # Recreate the table
        filepath = "./data/metadata/datasources.xlsx"
        df = pd.read_excel(filepath, index_col=0)
        admin.load(name="datasource", data=df)
        admin.commit()
        admin.close()
        df.reset_index(inplace=True)
        assert df.shape[0] == 10, print(df, df.columns)
        assert df.shape[1] == 17, print(df, df.columns)

        # Get an access object to read the table
        access = PGDao(test_credentials)
        df2 = access.read(table="datasource")
        assert isinstance(
            df2, pd.DataFrame), "DAOError: Get didn't return a dataframe"
        assert df2.shape[0] == 10, print(df2, df2.columns)
        assert df2.shape[1] == 17, print(df2, df2.columns)

    @ announce
    def test_column_exists(self):
        admin = TableAdmin(test_credentials)
        response = admin.column_exists(name='datasource', column='lifecycle')
        assert response, "Table error. Column exists failure"

    @ announce
    def test_get_columns(self):
        admin = TableAdmin(test_credentials)
        response = admin.get_columns(name='datasource')
        assert len(response) == 17, print(response)

    @announce
    def test_tables_teardown(self):
        dbadmin = DBAdmin(dba_credentials)
        if dbadmin.exists("test"):
            dbadmin.delete("test")
        dbadmin.commit()

        useradmin = UserAdmin(dba_credentials)
        if useradmin.exists(test_credentials.user):
            useradmin.revoke(test_credentials.user, test_pg_credentials.dbname)
            useradmin.delete(test_credentials.user)

        end(self)
