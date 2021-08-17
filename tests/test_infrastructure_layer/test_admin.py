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
# Modified : Tuesday, August 17th 2021, 4:49:24 am                            #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
import pytest
import logging
import pandas as pd

from src.infrastructure.data.admin import DBAdmin, TableAdmin
from src.infrastructure.data.context import PGDao
from src.infrastructure.data.connect import PGConnectionPool
from src.application.config import pg_login, test_login
from tests.test_utils.print import start, end
from tests.test_utils.debugging import announce
logger = logging.getLogger(__name__)
# --------------------------------------------------------------------------- #
tables = ["datasourceevent", "featuretransform",
          "countstats",
          "score", "model", "trainingevent", "prediction", "parameter", "countstats",
          "datasource", "feature", "dataset"]


dbname = "test"


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
            PGConnectionPool.return_connection(connection)

    @announce
    def test_exists(self, pg_connection):
        # First confirm database exists
        connection = pg_connection
        admin = DBAdmin()
        response = admin.exists(name=dbname, connection=connection)
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
        if admin.exists(dbname, connection):
            admin.delete(dbname, connection)
        admin.create(name=dbname, connection=connection)

    @announce
    def test_batch_create(self, pg_connection):
        connection = pg_connection

        # Create metadata tables
        filepath = "src/infrastructure/database/ddl/metadata/metadata_table_create.sql"
        tadmin = TableAdmin()
        tadmin.batch_create(filepath=filepath, connection=connection)

        # Confirm tables exist
        for table in tables:
            response = tadmin.exists(table, connection=connection)
            assert response, "TableError. Expected True Existence"

        # Get list of tables
        tablelist = tadmin.tables(
            test_login.dbname, connection=connection)
        if connection is not None:
            PGConnectionPool.return_connection(connection)
        assert len(tablelist) == 11, print(tablelist)

    @announce
    def test_delete(self, pg_connection):
        connection = pg_connection
        admin = TableAdmin()
        filepath = "src/infrastructure/database/ddl/metadata/metadata_table_drop.sql"
        admin.batch_delete(filepath=filepath, connection=connection)
        # Confirm table does exist
        for table in tables:
            alive = admin.exists(table, connection)
            assert not alive, "TableError. Expected False Existence"
        connection.commit()
        # Recreate the tables
        filepath = "src/infrastructure/database/ddl/metadata/metadata_table_create.sql"
        admin = TableAdmin()
        admin.batch_create(filepath=filepath, connection=connection)
        for table in tables:
            alive = admin.exists(table, connection)
            assert alive, "TableError. Expected True Existence"

    @ announce
    def test_column_exists(self, pg_connection):
        connection = pg_connection
        admin = TableAdmin()
        response = admin.column_exists(
            name='datasource', column='lifecycle', connection=connection)
        assert response, "Table error. Column exists failure"

    @ announce
    def test_get_columns(self, pg_connection):
        connection = pg_connection
        admin = TableAdmin()
        response = admin.get_columns(
            name='datasource', connection=connection)
        assert len(response) == 25, print(response)

    @ announce
    def test_tear_down(self, pg_connection):
        connection = pg_connection
        admin = TableAdmin()
        admin = TableAdmin()
        filepath = "src/infrastructure/database/ddl/metadata/metadata_table_drop.sql"
        admin.batch_delete(filepath=filepath, connection=connection)
        end(self)
