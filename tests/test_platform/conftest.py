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
# Modified : Friday, August 13th 2021, 9:28:24 am                             #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
import logging

import pytest
import pandas as pd

from src.platform.database.admin import DBAdmin, TableAdmin
from src.platform.database.connect import PGConnectionFactory
from src.platform.database.connect import SAConnectionFactory
from src.platform.config import pg_login, rx2m_test_login
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
    PGConnectionFactory.initialize(pg_login)
    connection = PGConnectionFactory.get_connection()
    connection.set_session(autocommit=True)
    return connection


@pytest.fixture(scope="class")
def rx2m_test_connection():
    """Builds a test database and returns a connection."""
    PGConnectionFactory.initialize(rx2m_test_login)
    connection = PGConnectionFactory.get_connection()
    connection.set_session(autocommit=True)
    return connection


@pytest.fixture(scope="class")
def sa_connection():
    """Builds a test database and returns a connection."""
    SAConnectionFactory.initialize(rx2m_test_login)
    return SAConnectionFactory.get_connection()
