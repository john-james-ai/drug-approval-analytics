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
# Modified : Wednesday, August 11th 2021, 5:37:57 am                          #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
import pytest
import pandas as pd

from src.platform.database.access import PGDao
from src.platform.config import dba_credentials
from tests import announce
# -----------------------------------------------------------------------------#


@pytest.mark.access
class AccessTests:

    @announce
    def test_setup(self, build_test_database, build_test_table):
        self._access = PGDao(dba_credentials)
        build_test_database
        build_test_table

    @announce
    def test_get_all_columns_all_rows(self, build_test_database,
                                      build_test_table):

        access = PGDao(dba_credentials)
        df = access.read(table="source")
        assert isinstance(
            df, pd.DataFrame), "DAOError: Get didn't return a dataframe"
        assert df.shape[0] == 10, "DAOError: Dataframe has no rows"
        assert df.shape[1] == 17, "DAOError: Dataframe has no columns"

    @announce
    def test_get(self, build_test_database,
                 build_test_table):
        build_test_database
        build_test_table
        access = PGDao(dba_credentials)
        df = access.read(table="source", columns=["version", "name", "uris"],
                         where_key="type", where_value='metadata')
        assert isinstance(
            df, pd.DataFrame), "DAOError: Get didn't return a dataframe"
        assert df.shape[0] == 2, "DAOError: Dataframe has no rows"
        assert df.shape[1] == 3, "DAOError: Dataframe has no columns"

    @announce
    def test_get_all_columns(self, build_test_database,
                             build_test_table):
        build_test_database
        build_test_table
        access = PGDao(dba_credentials)
        df = access.read(table="source",  where_key="type",
                         where_value="metadata")
        assert isinstance(
            df, pd.DataFrame), "DAOError: Get didn't return a dataframe"
        assert df.shape[0] == 2, "DAOError: Dataframe has no rows"
        assert df.shape[1] == 17, "DAOError: Dataframe has no columns"

    @announce
    def test_get_all_rows(self, build_test_database,
                          build_test_table):
        build_test_database
        build_test_table
        access = PGDao(dba_credentials)
        df = access.read(table="source", columns=["version", "name", "uris"])
        assert isinstance(
            df, pd.DataFrame), "DAOError: Get didn't return a dataframe"
        assert df.shape[0] == 10, "DAOError: Dataframe has no rows"
        assert df.shape[1] == 3, "DAOError: Dataframe has no columns"

    @announce
    def test_create(self, build_test_database,
                    build_test_table):
        columns = ['name', 'version', 'type',
                   'title', 'description', 'lifecycle']
        values = ['rhythm', 1, 'sound', 'Data Rhythm', 'Bounce data', 21]

        build_test_database
        build_test_table
        access = PGDao(dba_credentials)
        response = access.create(
            table="source", columns=columns, values=values)
        df = access.read(table="source")
        assert isinstance(
            df, pd.DataFrame), "DAOError: Get didn't return a dataframe"
        assert df.shape[0] == 11, "DAOError: Dataframe has no rows"
        assert df.shape[1] == 17, "DAOError: Dataframe has no columns"
        assert response.rowcount == 1, "DAOError: No rows updated"
        assert df[df.name == 'rhythm']['version'].values == 1, \
            "DAOError: Update failed."
        assert df[df.name == 'rhythm']['lifecycle'].values == 21, \
            "DAOError: Update failed."

    @announce
    def test_update(self, build_test_database,
                    build_test_table):
        build_test_database
        build_test_table
        access = PGDao(dba_credentials)
        response = access.update(table="source", column='version', value=99,
                                 where_key='name', where_value='studies')
        df = access.read(table="source", columns=["version", "name", "uris"],
                         where_key="name", where_value='studies')
        assert isinstance(
            df, pd.DataFrame), "DAOError: Get didn't return a dataframe"
        assert df.shape[0] == 1, "DAOError: Dataframe has no rows"
        assert df.shape[1] == 3, "DAOError: Dataframe has no columns"
        assert response.rowcount == 1, "DAOError: No rows updated"
        assert df[df.name == 'studies']['version'].values == 99, \
            "DAOError: Update failed."

    @announce
    def test_delete(self, build_test_database,
                    build_test_table):
        build_test_database
        build_test_table
        access = PGDao(dba_credentials)
        response = access.delete(table="source",
                                 where_key="name", where_value='rhythm')
        df = access.read(table="source")
        assert isinstance(
            df, pd.DataFrame), "DAOError: Get didn't return a dataframe"
        assert df.shape[0] == 10, "DAOError: Dataframe has no rows"
        assert df.shape[1] == 17, "DAOError: Dataframe has no columns"
        assert response.rowcount == 1, "DAOError: No rows updated"

    def test_transaction(self, build_test_database,
                         build_test_table):
        build_test_database
        build_test_table
        access = PGDao(dba_credentials)
        # Get current values before update
        before = access.read(table="source",
                             columns=["version", "name", "uris"],
                             where_key="name", where_value='studies')
        # Start the transaction
        access.begin()
        # Perform update
        response = access.update(table="source", column='version', value=99,
                                 where_key='name', where_value='studies')
        assert response.rowcount == 1, "Invalid number of rows updated"

        # Get data mid transaction
        mid = access.read(table="source", columns=["version", "name", "uris"],
                          where_key="name", where_value='studies')
        assert all(before == mid), "TestTransaction ValueError"

        # Commit changes
        access.commit()

        # Read post commit
        after = access.read(table="source",
                            columns=["version", "name", "uris"],
                            where_key="name", where_value='studies')
        assert any(after != mid), """TestTransaction ValueError: Update not reflected
            post commit"""
        assert after['version'].values == 99, """TestTransaction ValueError:
        Invalid value post update"""

    def test_teardown(self, tear_down):
        tear_down
