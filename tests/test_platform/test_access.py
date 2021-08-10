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
# Modified : Tuesday, August 10th 2021, 1:13:38 am                            #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
import pytest
import pandas as pd

from src.platform.database.access import PGDao
from src.platform.config import dba_credentials
# -----------------------------------------------------------------------------#


@pytest.mark.access
class AccessTests:

    def __init__(self, build_test_database, build_test_table):
        self._access = PGDao(dba_credentials.credentials)
        build_test_database
        build_test_table

    def test_get_all_columns_all_rows(self, build_test_database,
                                      build_test_table):

        access = PGDao(dba_credentials.credentials)
        df = access.read(table="sources")
        assert isinstance(
            df, pd.DataFrame), "DAOError: Get didn't return a dataframe"
        assert df.shape[0] == 10, "DAOError: Dataframe has no rows"
        assert df.shape[1] == 14, "DAOError: Dataframe has no columns"

    def test_get(self, build_test_database,
                 build_test_table):
        build_test_database
        build_test_table
        access = PGDao(dba_credentials.credentials)
        df = access.read(table="sources", columns=["version", "name", "uri"],
                         where_key="type", where_value='metadata')
        assert isinstance(
            df, pd.DataFrame), "DAOError: Get didn't return a dataframe"
        assert df.shape[0] == 2, "DAOError: Dataframe has no rows"
        assert df.shape[1] == 3, "DAOError: Dataframe has no columns"

    def test_get_all_columns(self, build_test_database,
                             build_test_table):
        build_test_database
        build_test_table
        access = PGDao(dba_credentials.credentials)
        df = access.read(table="sources",  where_key="type",
                         where_value="metadata")
        assert isinstance(
            df, pd.DataFrame), "DAOError: Get didn't return a dataframe"
        assert df.shape[0] == 2, "DAOError: Dataframe has no rows"
        assert df.shape[1] == 14, "DAOError: Dataframe has no columns"

    def test_get_all_rows(self, build_test_database,
                          build_test_table):
        build_test_database
        build_test_table
        access = PGDao(dba_credentials.credentials)
        df = access.read(table="sources", columns=["version", "name", "uri"])
        assert isinstance(
            df, pd.DataFrame), "DAOError: Get didn't return a dataframe"
        assert df.shape[0] == 10, "DAOError: Dataframe has no rows"
        assert df.shape[1] == 3, "DAOError: Dataframe has no columns"

    def test_create(self, build_test_database,
                    build_test_table):
        columns = ['name', 'version', 'type',
                   'title', 'description', 'lifecycle']
        values = ['rhythm', 1, 'sound', 'Data Rhythm', 'Bounce data', 21]

        build_test_database
        build_test_table
        access = PGDao(dba_credentials.credentials)
        rowcount = access.create(
            table="sources", columns=columns, values=values)
        df = access.read(table="sources")
        assert isinstance(
            df, pd.DataFrame), "DAOError: Get didn't return a dataframe"
        assert df.shape[0] == 11, "DAOError: Dataframe has no rows"
        assert df.shape[1] == 14, "DAOError: Dataframe has no columns"
        assert rowcount == 1, "DAOError: No rows updated"
        assert df[df.name == 'rhythm']['version'].values == 1, \
            "DAOError: Update failed."
        assert df[df.name == 'rhythm']['lifecycle'].values == 21, \
            "DAOError: Update failed."

    def test_load(self, build_test_database,
                  build_test_table):
        columns = ['name', 'version', 'type',
                   'title', 'description', 'lifecycle']
        values = ['rhythm', 1, 'sound', 'Data Rhythm', 'Bounce data', 21]

        build_test_database
        build_test_table
        access = PGDao(dba_credentials.credentials)
        rowcount = access.create(
            table="sources", columns=columns, values=values)
        df = access.read(table="sources")
        assert isinstance(
            df, pd.DataFrame), "DAOError: Get didn't return a dataframe"
        assert df.shape[0] == 11, "DAOError: Dataframe has no rows"
        assert df.shape[1] == 14, "DAOError: Dataframe has no columns"
        assert rowcount == 1, "DAOError: No rows updated"
        assert df[df.name == 'rhythm']['version'].values == 1, \
            "DAOError: Update failed."
        assert df[df.name == 'rhythm']['lifecycle'].values == 21, \
            "DAOError: Update failed."

    def test_update(self, build_test_database,
                    build_test_table):
        build_test_database
        build_test_table
        access = PGDao(dba_credentials.credentials)
        rowcount = access.update(table="sources", column='version', value=99,
                                 where_key='name', where_value='studies')
        df = access.read(table="sources", columns=["version", "name", "uri"],
                         where_key="name", where_value='studies')
        assert isinstance(
            df, pd.DataFrame), "DAOError: Get didn't return a dataframe"
        assert df.shape[0] == 1, "DAOError: Dataframe has no rows"
        assert df.shape[1] == 3, "DAOError: Dataframe has no columns"
        assert rowcount == 1, "DAOError: No rows updated"
        assert df[df.name == 'studies']['version'].values == 99, \
            "DAOError: Update failed."

    def test_delete(self, build_test_database,
                    build_test_table):
        build_test_database
        build_test_table
        access = PGDao(dba_credentials.credentials)
        rowcount = access.delete(table="sources",
                                 where_key="name", where_value='rhythm')
        df = access.read(table="sources")
        assert isinstance(
            df, pd.DataFrame), "DAOError: Get didn't return a dataframe"
        assert df.shape[0] == 10, "DAOError: Dataframe has no rows"
        assert df.shape[1] == 14, "DAOError: Dataframe has no columns"
        assert rowcount == 1, "DAOError: No rows updated"
