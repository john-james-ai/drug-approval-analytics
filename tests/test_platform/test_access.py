#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# =========================================================================== #
# Project  : Drug Approval Analytics                                          #
# Version  : 0.1.0                                                            #
# File     : \tests\test_data\test_access.py                                  #
# Language : Python 3.9.5                                                     #
# --------------------------------------------------------------------------  #
# Author   : John James                                                       #
# Company  : nov8.ai                                                          #
# Email    : john.james@nov8.ai                                               #
# URL      : https://github.com/john-james-sf/drug-approval-analytics         #
# --------------------------------------------------------------------------  #
# Created  : Sunday, August 8th 2021, 8:31:22 am                              #
# Modified : Sunday, August 8th 2021, 1:49:36 pm                              #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
import pytest
import pandas as pd

from src.platform.config import dba_credentials
from src.platform.database.access import PGDao
from src.platform.database.connections import PGConnector
# -----------------------------------------------------------------------------#


@pytest.mark.access
class AccessTests:

    def test_get_all_columns_all_rows(self, test_database):
        connection = test_database
        access = PGDao()
        df = access.get(table="sources", connection=connection)
        assert isinstance(
            df, pd.DataFrame), "DAOError: Get didn't return a dataframe"
        assert df.shape[0] == 10, "DAOError: Dataframe has no rows"
        assert df.shape[1] == 14, "DAOError: Dataframe has no columns"

    def test_get(self, test_database):
        connection = test_database
        access = PGDao()
        df = access.get(table="sources", columns=["version", "name", "uri"],
                        where_key="type", where_value='metadata',
                        connection=connection)
        assert isinstance(
            df, pd.DataFrame), "DAOError: Get didn't return a dataframe"
        assert df.shape[0] == 2, "DAOError: Dataframe has no rows"
        assert df.shape[1] == 3, "DAOError: Dataframe has no columns"

    def test_get_all_columns(self, test_database):
        connection = test_database
        access = PGDao()
        df = access.get(table="sources",  where_key="type",
                        where_value="metadata",
                        connection=connection)
        assert isinstance(
            df, pd.DataFrame), "DAOError: Get didn't return a dataframe"
        assert df.shape[0] == 2, "DAOError: Dataframe has no rows"
        assert df.shape[1] == 14, "DAOError: Dataframe has no columns"

    def test_get_all_rows(self, test_database):
        connection = test_database
        access = PGDao()
        df = access.get(table="sources", columns=["version", "name", "uri"],
                        connection=connection)
        assert isinstance(
            df, pd.DataFrame), "DAOError: Get didn't return a dataframe"
        assert df.shape[0] == 10, "DAOError: Dataframe has no rows"
        assert df.shape[1] == 3, "DAOError: Dataframe has no columns"
