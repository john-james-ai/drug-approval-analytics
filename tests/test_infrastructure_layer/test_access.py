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
# Modified : Monday, August 16th 2021, 4:03:09 am                             #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
import pytest
from datetime import datetime
import pandas as pd
import logging

from src.infrastructure.database.context import PGDao
from src.infrastructure.database.admin import DBAdmin, TableAdmin
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
    def test_read(self, access_database):
        connection = access_database
        access = PGDao(connection)
        df = access.read(name="datasource", columns=["version", "name", "uris"],
                         filter_key="type", filter_value='metadata')
        assert isinstance(
            df, pd.DataFrame), print("TestRead: TypeError.", df)
        assert df.shape[0] == 2, print("TestRead: Shape[0] incorrect.", df)
        assert df.shape[1] == 3, print("TestRead: Shape[1] incorrect.", df)

    @announce
    def test_read_iterator(self, access_database):
        connection = access_database
        access = PGDao(connection, "datasource")
        i = iter(access)
        n = next(i)
        assert isinstance(n, pd.DataFrame), print("TestIterator: Failed", n)

    @announce
    def test_get_all_data(self, access_database):

        connection = access_database
        access = PGDao(connection)
        df = access.read(name="datasource")
        assert isinstance(
            df, pd.DataFrame), print("TestGetAllData: TypeError.", df)
        assert df.shape[0] == 10, print("TestRead: Shape[0] incorrect.", df)
        assert df.shape[1] == 25, print("TestRead: Shape[1] incorrect.", df)

    @announce
    def test_get_all_columns(self, access_database):
        connection = access_database
        access = PGDao(connection)
        df = access.read(name="datasource",  filter_key="type",
                         filter_value="metadata")
        assert isinstance(
            df, pd.DataFrame), print("TestGetAllColumns: TypeError.", df)
        assert df.shape[0] == 2, print("TestRead: Shape[0] incorrect.", df)
        assert df.shape[1] == 25, print("TestRead: Shape[1] incorrect.", df)

    @announce
    def test_get_all_rows(self, access_database):
        connection = access_database

        access = PGDao(connection)
        df = access.read(name="datasource", columns=[
                         "version", "name", "uris"])
        assert isinstance(
            df, pd.DataFrame), print("TestGetAllRows: TypeError.", df)
        assert df.shape[0] == 10, print("TestRead: Shape[0] incorrect.", df)
        assert df.shape[1] == 3, print("TestRead: Shape[1] incorrect.", df)

    @announce
    def test_create(self, access_database):
        columns = ['name', 'version', 'type', 'webpage', 'link',
                   'title', 'description', 'lifecycle', 'frequency',
                   'creator', 'has_changed', 'created', 'created_by', 'link_type']
        values = ['rhythm', 1, 'sound', 'www.web.com', 'www.link.com',
                  'Data Rhythm', 'Bounce data', 21, 7, "john", False,
                  datetime.now(), 'JOhn', 'hcp']

        connection = access_database
        access = PGDao(connection)
        response = access.create(
            name="datasource", columns=columns, values=values)
        df = access.read(name="datasource")
        assert isinstance(
            df, pd.DataFrame), print("TestCreate: No dataframe returned.", df)
        assert df.shape[0] == 11, print("TestCreate: Shape[0] Incorrect.", df)
        assert df.shape[1] == 25, print("TestCreate: Shape[1] Incorrect.", df)
        assert response.rowcount == 1, print("TestCreate: ValueError.", df)
        assert df[df.name == 'rhythm']['version'].values == 1, \
            print("TestCreate: ValueError.", df)
        assert df[df.name == 'rhythm']['lifecycle'].values == 21, \
            print("TestCreate: ValueError.", df)

    @announce
    def test_update(self, access_database):
        connection = access_database
        access = PGDao(connection)
        response = access.update(name="datasource", column='version', value=99,
                                 filter_key='name', filter_value='studies')
        df = access.read(name="datasource", columns=["version", "name", "uris"],
                         filter_key="name", filter_value='studies')
        assert isinstance(
            df, pd.DataFrame), print("TestUpdate: TypeError", df)
        assert df.shape[0] == 1, print("TestUpdate: ValueError", df)
        assert df.shape[1] == 3, print("TestUpdate: ValueError", df)
        assert response.rowcount == 1, print("TestUpdate: ValueError", df)
        assert df[df.name == 'studies']['version'].values == 99, \
            print("TestUpdate: ValueError", df)

    @announce
    def test_delete(self, access_database):
        connection = access_database
        access = PGDao(connection)
        response = access.delete(name="datasource",
                                 filter_key="name", filter_value='rhythm')
        df = access.read(name="datasource")
        assert isinstance(
            df, pd.DataFrame), "DAOError: Get didn't return a dataframe"
        assert df.shape[0] == 10, "DAOError: Dataframe has no rows"
        assert df.shape[1] == 25, "DAOError: Dataframe has no columns"
        assert response.rowcount == 1, "DAOError: No rows updated"

    @announce
    def test_teardown(self, delete_tables, delete_user,
                      pg_connection, rx2m_test_connection):
        connection = rx2m_test_connection
        connection.close()
        delete_tables
        delete_user
        connection = pg_connection
        admin = DBAdmin()
        admin.terminate_database_processes("rx2m_test", connection)
        logger.debug("\n\nDatabase activity\n")
        # logger.debug(admin.activity(connection))
        # admin = UserAdmin()
        admin.delete(dbname, connection)
