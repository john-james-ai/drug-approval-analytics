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
# Modified : Sunday, August 8th 2021, 1:49:23 pm                              #
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
def test_database():
    """Builds a test database and returns a connection."""
    PGConnector.initialize(dba_credentials.credentials)
    connection = PGConnector.get_connection()
    dbadmin = DBAdmin()
    dbadmin.drop(connection=connection, name="test")
    dbadmin.create(connection=connection, name="test")

    # Drop the table if it exists.
    admin = TableAdmin()
    admin.drop(connection, "sources")

    # Get table data
    filepath = "./data/metadata/datasources.csv"
    df = pd.read_csv(filepath, index_col=0)

    # Get new SQLAlchemy connection since we are using pandas
    # to_sql facility to create the table.
    SAConnector.initialize(dba_credentials.credentials)
    sa_conn = SAConnector.get_connection()
    admin.create(connection=sa_conn, name='sources', data=df)
    SAConnector.return_connection(sa_conn)
    return connection
