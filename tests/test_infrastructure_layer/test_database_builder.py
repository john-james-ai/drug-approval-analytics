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
# Modified : Wednesday, August 18th 2021, 9:36:38 am                          #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
import pytest
from datetime import datetime
import pandas as pd
import logging

from src.infrastructure.data.database import MetaDatabaseBuilder, Database
from src.infrastructure.data.database import DatabaseConfiguration
from src.infrastructure.data.connect import Connection
from src.infrastructure.data.config import pg_pg_login, pg_rx2m_login
from src.infrastructure.data.config import j2_rx2m_login
from tests.test_utils.debugging import announce
logger = logging.getLogger(__name__)
# -----------------------------------------------------------------------------#
tables = ["datasourceevent", "featuretransform",
          "countstats", "score", "model", "trainingevent", "prediction",
          "parameter", "countstats", "datasource", "feature", "dataset"]


@pytest.mark.database_builder
class DatabaseBuilderTests:

    @announce
    def test_reset(self, builder):
        builder = builder

        db = Database()
        # Test reset
        builder.reset()
        # assert not db.exists(builder.config.name, connection),\
        #     print("BuildrReset: Database {} exists after reset".format(
        #         builder.config.name)
        # )

    @announce
    def test_build_database(self, builder):
        builder = builder

        db = Database()
        # Test build database
        builder.build_database()
        # assert db.exists(builder.config.name, connection),\
        #     print("BuildDatabase: Database {} does not exist.".format(
        #         builder.config.name)
        # )

    @announce
    def test_build_schema(self, builder):
        builder = builder

        db = Database()
        # Test build schema
        builder.build_schema()
        # assert db.schema_exists(builder.config.schema, connection),\
        #     print("BuildDatabase: Schema {} does not exist.".format(
        #         builder.config.schema)
        # )

    @announce
    def test_build_tables(self, builder):
        builder = builder

        db = Database()
        # Test build tables
        builder.build_tables()
        # for table in tables:
        #     assert db.table_exists(table, connection),\
        #         print("BuildDatabase: Table {} does not exist.".format(
        #             table)
        #     )

    @announce
    def test_initialize(self, builder):
        builder = builder
        #
        db = Database()
        # Test initialize
        builder.initialize()

    @announce
    def test_build_user(self, builder):
        builder = builder

        db = Database()
        # Test build owner
        builder.build_user()
        # assert db.user_exists(builder.config.user_db_credentials.use),\
        #     print("BuildDatabase: User {} does not exist.".format(
        #         builder.config.user_db_credentials.user)
        # )
