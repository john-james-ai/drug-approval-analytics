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
# Modified : Friday, August 13th 2021, 6:55:46 am                             #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
import pytest
import pandas as pd

from src.platform.database.access import PGDao
from src.platform.config import pg_login
from tests.test_utils.debugging import announce
# -----------------------------------------------------------------------------#


@pytest.mark.access
class AccessTests:
    @announce
    def test_get_all_columns_all_rows(self, build_test_database,
                                      build_test_table):
        connection = build_test_database
        build_test_table
        access = PGDao()
        df = access.read(table="datasource", connection=connection)
        assert isinstance(
            df, pd.DataFrame), "DAOError: Get didn't return a dataframe"
        assert df.shape[0] == 10, "DAOError: Dataframe has no rows"
        assert df.shape[1] == 17, "DAOError: Dataframe has no columns"

    @announce
    def test_get(self, build_test_database,
                 build_test_table):
        connection = build_test_database
        build_test_table
        access = PGDao()
        df = access.read(table="datasource", columns=["version", "name", "uris"],
                         where_key="type", where_value='metadata',
                         connection=connection)
        assert isinstance(
            df, pd.DataFrame), "DAOError: Get didn't return a dataframe"
        assert df.shape[0] == 2, "DAOError: Dataframe has no rows"
        assert df.shape[1] == 3, "DAOError: Dataframe has no columns"

    @announce
    def test_get_all_columns(self, build_test_database,
                             build_test_table):
        connection = build_test_database
        build_test_table
        access = PGDao()
        df = access.read(table="datasource",  where_key="type",
                         where_value="metadata", connection=connection)
        assert isinstance(
            df, pd.DataFrame), "DAOError: Get didn't return a dataframe"
        assert df.shape[0] == 2, "DAOError: Dataframe has no rows"
        assert df.shape[1] == 17, "DAOError: Dataframe has no columns"

    @announce
    def test_get_all_rows(self, build_test_database,
                          build_test_table):
        connection = build_test_database
        build_test_table
        access = PGDao()
        df = access.read(table="datasource", columns=["version", "name", "uris"],
                         connection=connection)
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

        connection = build_test_database
        build_test_table
        access = PGDao()
        response = access.create(
            table="datasource", columns=columns, values=values,
            connection=connection)
        df = access.read(table="datasource", connection=connection)
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
        connection = build_test_database
        build_test_table
        access = PGDao()
        response = access.update(table="datasource", column='version', value=99,
                                 where_key='name', where_value='studies', connection=connection)
        df = access.read(table="datasource", columns=["version", "name", "uris"],
                         where_key="name", where_value='studies', connection=connection)
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
        connection = build_test_database
        build_test_table
        access = PGDao()
        response = access.delete(table="datasource",
                                 where_key="name", where_value='rhythm',
                                 connection=connection)
        df = access.read(table="datasource", connection=connection)
        assert isinstance(
            df, pd.DataFrame), "DAOError: Get didn't return a dataframe"
        assert df.shape[0] == 10, "DAOError: Dataframe has no rows"
        assert df.shape[1] == 17, "DAOError: Dataframe has no columns"
        assert response.rowcount == 1, "DAOError: No rows updated"

    def test_teardown(self, tear_down):
        tear_down
