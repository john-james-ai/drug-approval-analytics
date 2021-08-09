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
# Modified : Monday, August 9th 2021, 4:27:20 pm                              #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
import pytest
import pandas as pd

from src.platform.database.admin import DBAdmin, TableAdmin
from src.platform.database.connections import SAConnector, PGConnector
from src.platform.config import dba_credentials
# --------------------------------------------------------------------------  #


@pytest.fixture(scope="class")
def get_connection():
    PGConnector.initialize(dba_credentials.credentials)
    connection = PGConnector.get_connection()
    return connection


@pytest.fixture(scope="class")
def build_test_database():
    """Builds a test database and returns a connection."""
    PGConnector.initialize(dba_credentials.credentials)
    connection = PGConnector.get_connection()
    dbadmin = DBAdmin()
    dbadmin.drop(connection=connection, name="test")
    dbadmin.create(connection=connection, name="test")
    return connection


@pytest.fixture(scope="class")
def build_test_table():
    # Drop the table if it exists.
    PGConnector.initialize(dba_credentials.credentials)
    connection = PGConnector.get_connection()
    admin = TableAdmin()
    admin.drop(connection, "sources")
    connection.commit()
    # Get table data
    filepath = "./data/metadata/datasources.csv"
    df = pd.read_csv(filepath, index_col=0)

    # Get new SQLAlchemy connection since we are using pandas
    # to_sql facility to create the table.
    SAConnector.initialize(dba_credentials.credentials)
    sa_conn = SAConnector.get_connection()
    admin.create(connection=sa_conn, name='sources', data=df)
    SAConnector.return_connection(sa_conn)


@pytest.fixture(scope="class")
def drop_test_database():
    """Builds a test database and returns a connection."""
    PGConnector.initialize(dba_credentials.credentials)
    connection = PGConnector.get_connection()
    dbadmin = DBAdmin()
    dbadmin.drop(connection=connection, name="test")
    connection.commit()
