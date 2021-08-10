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
# Modified : Tuesday, August 10th 2021, 3:42:37 am                            #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
import pytest
import pandas as pd

from src.platform.database.admin import DBAdmin, TableAdmin
from src.platform.database.access import PGDao
from src.platform.config import dba_credentials
from tests.test_utils.print import start, end
from tests import announce

# --------------------------------------------------------------------------- #
tables = ["datasource", "dataset", "feature", "statistic", "score",
          "prediction", "datasourceevent", "parameter", "featuretransform",
          "trainingevent", "model"]


@pytest.mark.dbadmin
class DBAdminTests:

    @announce
    @pytest.mark.dbadmin
    def test_setup(self):
        start(self)
        test = "test"
        admin = DBAdmin(dba_credentials.credentials)
        admin.delete(test)

    @announce
    @pytest.mark.dbadmin
    def test_create_database(self):
        test = 'test'
        # First confirm database doesn't exist
        admin = DBAdmin(dba_credentials.credentials)
        response = admin.exists(name=test)
        assert not response, "Database Exists Error: Expected False"

        # Create database and confirm existence
        admin.create(name=test)
        response = admin.exists(name=test)
        assert response, "Database Exists Error: Expected True"

    @announce
    def test_delete_database(self):
        # First confirm database does exist
        test = 'test'
        admin = DBAdmin(dba_credentials.credentials)
        response = admin.exists(name=test)
        assert response, "Database Exists Error: Expected True"

        # Create database and confirm existence
        admin.delete(name=test)
        response = admin.exists(name=test)
        assert not response, "Database Exists Error: Expected False"
        end(self)


class TableAdminTests:

    def test_setup(self):
        start(self)
        admin = TableAdmin(dba_credentials.credentials)
        for table in tables:
            admin.delete(table)

    @announce
    def test_create_table(self):
        # Confirm table does not exist
        admin = TableAdmin(dba_credentials.credentials)
        for table in tables:
            response = admin.exists(table)
            assert not response, "TableError. Expected False Existence"

        # Create table and confirm existence
        filepath = "src/platform/metadata/metadata_table_create.sql"
        admin.create(filepath=filepath)
        for table in tables:
            response = admin.exists(table)
            assert response, "TableError. Expected True Existence"

    @announce
    def test_delete_table(self):
        # Confirm table does exist

        admin = TableAdmin(dba_credentials.credentials)
        for table in tables:
            response = admin.exists(table)
            assert response, "TableError. Expected True Existence"

        # Delete all table and confirm existence
        admin = TableAdmin(dba_credentials.credentials)
        for table in tables:
            admin.delete(table)

        for table in tables:
            response = admin.exists(table)
            assert not response, "TableError. Expected False Existence"

    @announce
    def test_load_table(self):
        test = "source"
        admin = TableAdmin(dba_credentials.credentials)
        # Ensure the table doesn't exist
        admin.delete(test)

        # Recreate the table
        filepath = "./data/metadata/datasources.csv"
        df = pd.read_csv(filepath)
        admin.load(name=test, data=df)

        # Get an access object to read the table
        access = PGDao(dba_credentials.credentials)
        df = access.read(table="source")
        assert isinstance(
            df, pd.DataFrame), "DAOError: Get didn't return a dataframe"
        assert df.shape[0] == 10, "DAOError: Dataframe has no rows"
        assert df.shape[1] == 15, "DAOError: Dataframe has no columns"

    @announce
    def test_column_exists(self):
        test = "source"
        admin = TableAdmin(dba_credentials.credentials)
        response = admin.column_exists(name=test, column='lifecycle')
        assert response, "Table error. Column exists failure"

    @announce
    def test_get_columns(self):
        test = "source"
        admin = TableAdmin(dba_credentials.credentials)
        response = admin.get_columns(name=test)
        assert len(response) == 14, "TableError, Get columns failure."

    @announce
    def test_add_columns(self):
        admin = TableAdmin(dba_credentials.credentials)
        test = "source"
        admin.add_column(name=test, column="figit", datatype="INTEGER")
        response = admin.get_columns(name=test)
        assert len(response) == 15, "TableError, Add column failure."
        end(self)
