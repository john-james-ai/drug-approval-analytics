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
# Modified : Sunday, August 15th 2021, 1:46:44 am                             #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
import pytest
from datetime import datetime
import pandas as pd
import logging

from src.platform.database.context import PGDao
from src.platform.database.admin import DBAdmin, TableAdmin
from tests.test_utils.debugging import announce
logger = logging.getLogger(__name__)
# -----------------------------------------------------------------------------#
tables = ["datasourceevent", "featuretransform",
          "countstats",
          "score", "model", "trainingevent", "prediction", "parameter", "countstats",
          "datasource", "feature", "dataset"]


dbname = "rx2m_test"


@pytest.mark.context
class AccessTests:

    @announce
    def test_setup(self, pg_connection):
        connection = pg_connection
        # Cleanup
        admin = DBAdmin()
        admin.terminate_database_processes(
            name=dbname, connection=connection)
        if admin.exists(dbname, connection):
            admin.delete(name=dbname, connection=connection)

    @announce
    def test_build_database(self, pg_connection):
        connection = pg_connection
        # Build Database
        admin = DBAdmin()
        admin.create(name=dbname, connection=connection)

    @announce
    def test_create_tables(self, rx2m_test_connection):
        connection = rx2m_test_connection
        # Build the tables
        connection = rx2m_test_connection
        filepath = "src/platform/metadata/metadata_table_create.sql"
        admin = TableAdmin()
        admin.create(filepath=filepath, connection=connection)

    @announce
    def test_load_tables(self, sa_connection):
        # Load data
        connection = sa_connection
        filepath = "./data/metadata/datasources.xlsx"
        df = pd.read_excel(filepath, index_col=0)
        admin = TableAdmin()
        admin.load(name="datasource", data=df, connection=connection)

    @announce
    def test_get_all_columns_all_rows(self, rx2m_test_connection):

        connection = rx2m_test_connection
        access = PGDao()
        df = access.read(table="datasource", connection=connection)
        assert isinstance(
            df, pd.DataFrame), "DAOError: Get didn't return a dataframe"
        assert df.shape[0] == 10, "DAOError: Dataframe has no rows"
        assert df.shape[1] == 25, "DAOError: Dataframe has no columns"

    @announce
    def test_get(self, rx2m_test_connection):
        connection = rx2m_test_connection
        access = PGDao()
        df = access.read(table="datasource", columns=["version", "name", "uris"],
                         filter_key="type", filter_value='metadata',
                         connection=connection)
        assert isinstance(
            df, pd.DataFrame), "DAOError: Get didn't return a dataframe"
        assert df.shape[0] == 2, "DAOError: Dataframe has no rows"
        assert df.shape[1] == 3, "DAOError: Dataframe has no columns"

    @announce
    def test_get_all_columns(self, rx2m_test_connection):
        connection = rx2m_test_connection
        access = PGDao()
        df = access.read(table="datasource",  filter_key="type",
                         filter_value="metadata", connection=connection)
        assert isinstance(
            df, pd.DataFrame), "DAOError: Get didn't return a dataframe"
        assert df.shape[0] == 2, "DAOError: Dataframe has no rows"
        assert df.shape[1] == 25, "DAOError: Dataframe has no columns"

    @announce
    def test_get_all_rows(self, rx2m_test_connection):
        connection = rx2m_test_connection

        access = PGDao()
        df = access.read(table="datasource", columns=["version", "name", "uris"],
                         connection=connection)
        assert isinstance(
            df, pd.DataFrame), "DAOError: Get didn't return a dataframe"
        assert df.shape[0] == 10, "DAOError: Dataframe has no rows"
        assert df.shape[1] == 3, "DAOError: Dataframe has no columns"

    @announce
    def test_create(self, rx2m_test_connection):
        columns = ['name', 'version', 'type', 'webpage', 'link',
                   'title', 'description', 'lifecycle', 'frequency',
                   'creator', 'has_changed', 'created', 'created_by', 'link_type']
        values = ['rhythm', 1, 'sound', 'www.web.com', 'www.link.com',
                  'Data Rhythm', 'Bounce data', 21, 7, "john", False,
                  datetime.now(), 'JOhn', 'hcp']

        connection = rx2m_test_connection
        access = PGDao()
        response = access.create(
            table="datasource", columns=columns, values=values,
            connection=connection)
        df = access.read(table="datasource", connection=connection)
        assert isinstance(
            df, pd.DataFrame), "DAOError: Get didn't return a dataframe"
        assert df.shape[0] == 11, "DAOError: Dataframe has no rows"
        assert df.shape[1] == 25, "DAOError: Dataframe has no columns"
        assert response.rowcount == 1, "DAOError: No rows updated"
        assert df[df.name == 'rhythm']['version'].values == 1, \
            "DAOError: Update failed."
        assert df[df.name == 'rhythm']['lifecycle'].values == 21, \
            "DAOError: Update failed."

    @announce
    def test_update(self, rx2m_test_connection):
        connection = rx2m_test_connection
        access = PGDao()
        response = access.update(table="datasource", column='version', value=99,
                                 filter_key='name', filter_value='studies', connection=connection)
        df = access.read(table="datasource", columns=["version", "name", "uris"],
                         filter_key="name", filter_value='studies', connection=connection)
        assert isinstance(
            df, pd.DataFrame), "DAOError: Get didn't return a dataframe"
        assert df.shape[0] == 1, "DAOError: Dataframe has no rows"
        assert df.shape[1] == 3, "DAOError: Dataframe has no columns"
        assert response.rowcount == 1, "DAOError: No rows updated"
        assert df[df.name == 'studies']['version'].values == 99, \
            "DAOError: Update failed."

    @announce
    def test_delete(self, rx2m_test_connection):
        connection = rx2m_test_connection
        access = PGDao()
        response = access.delete(table="datasource",
                                 filter_key="name", filter_value='rhythm',
                                 connection=connection)
        df = access.read(table="datasource", connection=connection)
        assert isinstance(
            df, pd.DataFrame), "DAOError: Get didn't return a dataframe"
        assert df.shape[0] == 10, "DAOError: Dataframe has no rows"
        assert df.shape[1] == 25, "DAOError: Dataframe has no columns"
        assert response.rowcount == 1, "DAOError: No rows updated"

    @announce
    def test_teardown(self, pg_connection):
        connection = pg_connection
        admin = TableAdmin()
        for table in tables:
            admin.delete(table, connection)
        admin = DBAdmin()
        admin.terminate_database_processes(
            name=dbname, connection=connection)
        admin.delete(dbname, connection)
