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
# Modified : Thursday, August 12th 2021, 7:20:32 am                           #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
import logging

import pytest
import pandas as pd

from src.platform.database.admin import DBAdmin, TableAdmin, UserAdmin
from src.platform.config import dba_credentials, test_pg_credentials
from src.platform.config import test_credentials
# --------------------------------------------------------------------------- #
logger = logging.getLogger(__name__)


@pytest.fixture(scope="class")
def build_test_metadata_database():
    """Builds a test database and returns a connection."""

    # Create test user
    useradmin = UserAdmin(dba_credentials)
    useradmin.create(test_pg_credentials.user, test_pg_credentials.password)
    useradmin.grant(test_pg_credentials.user, dba_credentials.dbname)
    useradmin.commit()
    useradmin.close()
    logger.debug(
        "\n\n\t\t\tBuild Test Metadata Database: Created test user and committed.\n")

    # New user creates database
    dbadmin = DBAdmin(test_pg_credentials)
    dbadmin.create(name="test")
    dbadmin.commit()
    dbadmin.close()
    logger.debug(
        "\n\n\t\t\tBuild Test Metadata Database: Test user created database and committed.\n")

    # Create the metadata tables
    tadmin = TableAdmin(test_credentials)
    filepath = "src/platform/metadata/metadata_table_create.sql"
    tadmin.create(filepath=filepath)
    logger.debug(
        "\n\n\t\t\tBuild Test Metadata Database: Created metadata tables.\n")

    # Load data into datasource table
    filepath = "data/metadata/datasources.xlsx"
    df = pd.read_excel(io=filepath, index_col=0)
    df['uris'] = df['uris'].apply(eval)
    tadmin.load(name='datasource', data=df)
    tadmin.commit()
    tadmin.close()
    logger.debug(
        "\n\n\t\t\tBuild Test Metadata Database: Loaded data sources.\n")
    logger.debug(
        "\n\n\t\t\tBuild Test Metadata Database: Complete.\n")


@pytest.fixture(scope="class")
def tear_down_test_metadata_database():
    """Returns the database to pretest state."""
    logger.debug("\n\n\t\t\tTear Down Test Metadata Database\n")
    dbadmin = DBAdmin(test_pg_credentials)
    dbadmin.delete("test")
    dbadmin.commit()
    dbadmin.close()
    logger.debug(
        "\n\n\t\t\tTear Down Test Metadata Database: Deleted test database\n")

    useradmin = UserAdmin(dba_credentials)
    useradmin.revoke(test_pg_credentials.user, test_pg_credentials.dbname)
    logger.debug(
        "\n\n\t\t\tTear Down Test Metadata Database: Revoked privileges\n")
    useradmin.delete(test_credentials.user)
    useradmin.commit()
    useradmin.close()
    logger.debug(
        "\n\n\t\t\tTear Down Test Metadata Database: Deleted user and committed\n")
