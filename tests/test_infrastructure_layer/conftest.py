#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# =========================================================================== #
# Project  : Drug Approval Analytics                                          #
# Version  : 0.1.0                                                            #
# File     : \conftest.py                                                     #
# Language : Python 3.9.5                                                     #
# --------------------------------------------------------------------------  #
# Author   : John James                                                       #
# Company  : nov8.ai                                                          #
# Email    : john.james@nov8.ai                                               #
# URL      : https://github.com/john-james-sf/drug-approval-analytics         #
# --------------------------------------------------------------------------  #
# Created  : Sunday, August 8th 2021, 8:37:47 am                              #
# Modified : Monday, August 16th 2021, 3:42:58 am                             #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
import logging

import pytest
import pandas as pd

from src.infrastructure.database.admin import DBAdmin, TableAdmin, UserAdmin
from src.infrastructure.database.connect import PGConnectionPool
from src.infrastructure.database.connect import SAConnectionPool
from src.infrastructure.database.connect import Connection
from src.application.config import pg_login, rx2m_test_login
# --------------------------------------------------------------------------- #
logger = logging.getLogger(__name__)
# --------------------------------------------------------------------------- #
tables = ["datasourceevent", "featuretransform",
          "countstats",
          "score", "model", "trainingevent", "prediction", "parameter", "countstats",
          "datasource", "feature", "dataset"]


dbname = "rx2m_test"


@pytest.fixture(scope="class")
def pg_connection():
    """Builds a test database and returns a connection."""
    PGConnectionPool.initialize(pg_login)
    connection = PGConnectionPool.get_connection()
    connection.set_session(autocommit=True)
    return connection


@pytest.fixture(scope="class")
def rx2m_test_connection():
    """Builds a test database and returns a connection."""
    PGConnectionPool.initialize(rx2m_test_login)
    connection = PGConnectionPool.get_connection()
    connection.set_session(autocommit=True)
    return connection


@pytest.fixture(scope="class")
def sa_connection():
    """Builds a test database and returns a connection."""
    SAConnectionPool.initialize(rx2m_test_login)
    return SAConnectionPool.get_connection()


@pytest.fixture(scope="class")
def create_database(pg_connection):
    connection = pg_connection
    admin = DBAdmin()
    admin.delete(rx2m_test_login.dbname, connection)
    admin.create(rx2m_test_login.dbname, connection)


@pytest.fixture(scope="class")
def delete_database(pg_connection):
    connection = pg_connection
    admin = DBAdmin()
    admin.delete(rx2m_test_login.dbname, connection)


@pytest.fixture(scope="class")
def create_tables(rx2m_test_connection):
    connection = rx2m_test_connection
    admin = TableAdmin()
    if admin.exists("parameter", connection):
        filepath = "src/infrastructure/database/ddl/metadata/metadata_table_drop.sql"
        admin.batch_delete(filepath, connection)
    filepath = "src/infrastructure/database/ddl/metadata/metadata_table_create.sql"
    admin.batch_create(filepath, connection)


@pytest.fixture(scope="class")
def delete_tables(rx2m_test_connection):
    connection = rx2m_test_connection
    admin = TableAdmin()
    filepath = "src/infrastructure/database/ddl/metadata/metadata_table_drop.sql"
    admin.batch_delete(filepath, connection)


@pytest.fixture(scope="class")
def create_user(pg_connection):
    connection = pg_connection
    admin = UserAdmin()
    if admin.exists(rx2m_test_login.user, connection):
        admin.revoke(rx2m_test_login.user, rx2m_test_login.dbname, connection)
        admin.delete(rx2m_test_login.user, connection)
    admin.create(rx2m_test_login.user, rx2m_test_login.password, connection)
    admin.grant(rx2m_test_login.user, rx2m_test_login.dbname, connection)


@pytest.fixture(scope="class")
def delete_user(pg_connection):
    connection = pg_connection
    admin = UserAdmin()
    if admin.exists(rx2m_test_login.user, connection):
        admin.revoke(rx2m_test_login.user, rx2m_test_login.dbname, connection)
        admin.delete(rx2m_test_login.user, connection)


@pytest.fixture(scope="class")
def load_table(rx2m_test_connection, sa_connection):
    connection = rx2m_test_connection
    connection_sa = sa_connection
    admin = TableAdmin()
    filepath = "./data/metadata/datasources.xlsx"
    df = pd.read_excel(filepath, index_col=0)
    admin.load("datasource", df, connection_sa)
    return connection


@pytest.fixture(scope="class")
def access_database(create_database, create_user, create_tables, load_table):
    """Builds access database for testing database access and repositories."""
    create_database
    create_tables
    create_user
    connection = load_table
    return connection
