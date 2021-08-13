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
# Modified : Thursday, August 12th 2021, 2:50:25 am                           #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
import pytest
import pandas as pd

from src.platform.database.admin import DBAdmin, TableAdmin, UserAdmin
from src.platform.config import dba_credentials, test_credentials
from src.platform.config import test_pg_credentials
# --------------------------------------------------------------------------  #


@pytest.fixture(scope="class")
def build_test_database():
    """Builds a test database and returns a connection."""
    admin = DBAdmin(dba_credentials)
    admin.delete(name="test")
    admin.create(name="test")


@pytest.fixture(scope="class")
def build_test_table():
    # Drop the table if it exists.
    admin = TableAdmin(dba_credentials)
    admin.delete(name="source")

    # Get table data
    filepath = "./data/metadata/datasources.xlsx"
    df = pd.read_excel(filepath, index_col=0)

    # Load the data
    admin.load(name='source', data=df)
    admin.close()


@pytest.fixture(scope="class")
def drop_test_database():
    """Builds a test database and returns a connection."""
    admin = DBAdmin(dba_credentials)
    admin.drop(name="test")
    admin.close()


@pytest.fixture(scope="class")
def tear_down():
    """Returns the database to pretest state."""
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
