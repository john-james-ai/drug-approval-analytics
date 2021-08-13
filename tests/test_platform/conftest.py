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
# Modified : Friday, August 13th 2021, 6:35:22 am                             #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
import logging

import pytest
import pandas as pd

from src.platform.database.connect import PGConnectionFactory
from src.platform.database.connect import SAConnectionFactory
from src.platform.database.admin import DBAdmin, UserAdmin
from src.platform.config import dba_credentials, test_pg_credentials
from src.platform.config import test_credentials
# --------------------------------------------------------------------------- #
logger = logging.getLogger(__name__)


@pytest.fixture(scope="class")
def dba_connection():
    """Builds a test database and returns a connection."""
    PGConnectionFactory.initialize(dba_credentials)
    connection = PGConnectionFactory.get_connection()
    connection.set_session(autocommit=True)
    return connection


@pytest.fixture(scope="class")
def test_pg_connection():
    """Builds a test database and returns a connection."""
    PGConnectionFactory.initialize(test_pg_credentials)
    connection = PGConnectionFactory.get_connection()
    connection.set_session(autocommit=True)
    return connection


@pytest.fixture(scope="class")
def test_connection():
    """Builds a test database and returns a connection."""
    PGConnectionFactory.initialize(test_credentials)
    connection = PGConnectionFactory.get_connection()
    connection.set_session(autocommit=True)
    return connection


@pytest.fixture(scope="class")
def sa_connection():
    """Builds a test database and returns a connection."""
    SAConnectionFactory.initialize(test_credentials)
    return SAConnectionFactory.get_connection()


@pytest.fixture(scope="class")
def tear_down(dba_connection):
    """Returns the database to pretest state."""
    connection = dba_connection
    useradmin = UserAdmin()
    if useradmin.exists(name=test_credentials.user,
                        connection=connection):
        useradmin.revoke(name=test_credentials.user,
                         dbname=test_credentials.dbname,
                         connection=connection)

    if useradmin.exists(name=test_pg_credentials.user,
                        connection=connection):
        useradmin.revoke(name=test_pg_credentials.user,
                         dbname=test_credentials.dbname,
                         connection=connection)
        useradmin.revoke(name=test_pg_credentials.user,
                         dbname=dba_credentials.dbname,
                         connection=connection)

    dbadmin = DBAdmin()
    if dbadmin.exists("test", connection):
        dbadmin.terminate_database_processes("test", connection)
        dbadmin.delete("test", connection)

    useradmin.delete(name=test_credentials.user,
                     connection=connection)
    useradmin.delete(name=test_pg_credentials.user,
                     connection=connection)


@pytest.fixture(scope="class")
def onboard_user(dba_connection, test_pg_connection, test_connection):
    # Create test user
    logger.debug("\n\n\t\t\tOnboarding User\n")
    connection = dba_connection
    admin = UserAdmin()
    admin.create(test_pg_credentials.user,
                 test_pg_credentials.password, connection)
    logger.debug("\n\n\t\t\tCreated test_pg\n")
    # Grant privileges on postgres
    useradmin = UserAdmin()
    useradmin.grant(test_pg_credentials.user,
                    dba_credentials.dbname, connection)
    logger.debug("\n\n\t\t\tGranted privileges to test_pg\n")

    # User creates test database on own credentials
    connection = test_pg_connection
    dbadmin = DBAdmin()
    dbadmin.create("test", connection)
    logger.debug("\n\n\t\t\tTest created test database\n")

    # User creates connection to new database
    logger.debug("\n\n\t\t\tReturning the connection\n")
    connection = test_pg_connection

    return connection


@pytest.fixture(scope="class")
def tear_down_test_metadata_database(dba_connection, test_pg_connection,
                                     test_connection):
    """Returns the database to pretest state."""
    connection = test_pg_connection
    logger.debug("\n\n\t\t\tTear Down Test Metadata Database\n")
    dbadmin = DBAdmin()
    dbadmin.delete("test", connection)
    connection.commit()
    PGConnectionFactory.return_connection(connection)
    logger.debug(
        "\n\n\t\t\tTear Down Test Metadata Database: Deleted test database\n")

    connection = dba_connection
    useradmin = UserAdmin()
    useradmin.revoke_user(test_pg_credentials.user,
                          test_pg_credentials.dbname, connection)
    logger.debug(
        "\n\n\t\t\tTear Down Test Metadata Database: Revoked privileges\n")
    useradmin.delete_user(test_credentials.user, connection)
    connection.commit()
    PGConnectionFactory.return_connection(connection)
    logger.debug(
        "\n\n\t\t\tTear Down Test Metadata Database: Deleted user and committed\n")
