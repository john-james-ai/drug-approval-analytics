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
# Modified : Sunday, August 15th 2021, 1:46:44 am                             #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
import pytest
import logging
import pandas as pd

from src.platform.database.admin import DBAdmin, TableAdmin
from src.platform.database.context import PGDao
from src.platform.database.connect import PGConnectionFactory
from src.platform.config import pg_login, rx2m_test_login
from tests.test_utils.print import start, end
from tests.test_utils.debugging import announce
logger = logging.getLogger(__name__)
# --------------------------------------------------------------------------- #
tables = ["datasourceevent", "featuretransform",
          "countstats",
          "score", "model", "trainingevent", "prediction", "parameter", "countstats",
          "datasource", "feature", "dataset"]


dbname = "rx2m_test"


@pytest.mark.dbadmin
class DBAdminTests:

    @announce
    def test_setup(self, pg_connection):
        start(self)
        connection = pg_connection
        # Create database and confirm existence
        admin = DBAdmin()
        admin.terminate_database_processes(
            name=dbname, connection=connection)
        if admin.exists(dbname, connection):
            admin.delete(name=dbname, connection=connection)
            response = admin.exists(name=dbname, connection=connection)
            assert not response, "Database Exists Error: Expected False"

    @announce
    def test_create(self, pg_connection):

        connection = pg_connection
        # Create database and confirm existence
        admin = DBAdmin()
        admin.create(name=dbname, connection=connection)
        if connection is not None:
            PGConnectionFactory.return_connection(connection)

    @announce
    def test_exists(self, rx2m_test_connection):
        # First confirm database exists
        connection = rx2m_test_connection
        admin = DBAdmin()
        response = admin.exists(name=dbname, connection=connection)
        if connection is not None:
            PGConnectionFactory.return_connection(connection)
        assert response, "Database Exists Error: Expected True"

    @announce
    def test_delete(self, pg_connection):
        # Create database and confirm existence
        connection = pg_connection
        admin = DBAdmin()
        admin.terminate_database_processes(
            name=dbname, connection=connection)
        admin.delete(name=dbname, connection=connection)
        response = admin.exists(name=dbname, connection=connection)
        assert not response, "Database Exists Error: Expected False"


class TableAdminTests:

    @announce
    def test_setup(self, pg_connection):
        start(self)
        connection = pg_connection
        # Create database and confirm existence
        admin = DBAdmin()
        admin.create(name=dbname, connection=connection)

    @announce
    def test_create(self, rx2m_test_connection):
        connection = rx2m_test_connection

        # Create metadata tables
        filepath = "src/platform/metadata/metadata_table_create.sql"
        tadmin = TableAdmin()
        tadmin.create(filepath=filepath, connection=connection)

        # Confirm tables exist
        for table in tables:
            response = tadmin.exists(table, connection=connection)
            assert response, "TableError. Expected True Existence"

        # Get list of tables
        tablelist = tadmin.tables(
            rx2m_test_login.dbname, connection=connection)
        if connection is not None:
            PGConnectionFactory.return_connection(connection)
        assert len(tablelist) == 11, print(tablelist)

    @announce
    def test_delete(self, rx2m_test_connection):
        connection = rx2m_test_connection
        admin = TableAdmin()
        filepath = "src/platform/metadata/metadata_table_drop.sql"
        admin.batch_delete(filepath=filepath, connection=connection)
        # Confirm table does exist
        for table in tables:
            alive = admin.exists(table, connection)
            assert not alive, "TableError. Expected False Existence"
        connection.commit()
        # Recreate the tables
        filepath = "src/platform/metadata/metadata_table_create.sql"
        admin = TableAdmin()
        admin.create(filepath=filepath, connection=connection)
        for table in tables:
            alive = admin.exists(table, connection)
            assert alive, "TableError. Expected True Existence"

    @announce
    def test_load(self, sa_connection):
        connection = sa_connection
        admin = TableAdmin()

        # Load data
        filepath = "./data/metadata/datasources.xlsx"
        df = pd.read_excel(filepath, index_col=0)
        admin.load(name="datasource", data=df, connection=connection)

        df.reset_index(inplace=True)
        assert df.shape[0] == 10, print(df, df.columns)
        assert df.shape[1] == 24, print(df, df.columns)

    @ announce
    def test_read(self, rx2m_test_connection):
        connection = rx2m_test_connection

        # Get an access object to read the table
        access = PGDao()
        df2 = access.read(table="datasource", connection=connection)
        assert isinstance(
            df2, pd.DataFrame), "DAOError: Get didn't return a dataframe"
        assert df2.shape[0] == 10, print(df2, df2.columns)
        assert df2.shape[1] == 25, print(df2, df2.columns)

    @ announce
    def test_column_exists(self, rx2m_test_connection):
        connection = rx2m_test_connection
        admin = TableAdmin()
        response = admin.column_exists(
            name='datasource', column='lifecycle', connection=connection)
        assert response, "Table error. Column exists failure"

    @ announce
    def test_get_columns(self, rx2m_test_connection):
        connection = rx2m_test_connection
        admin = TableAdmin()
        response = admin.get_columns(
            name='datasource', connection=connection)
        assert len(response) == 25, print(response)

    @ announce
    def test_tear_down(self, rx2m_test_connection):
        connection = rx2m_test_connection
        admin = TableAdmin()
        admin = TableAdmin()
        filepath = "src/platform/metadata/metadata_table_drop.sql"
        admin.batch_delete(filepath=filepath, connection=connection)
        end(self)
