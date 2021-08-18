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
# Modified : Wednesday, August 18th 2021, 8:48:46 am                          #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
import logging

import pytest
import pandas as pd
from src.infrastructure.data.connect import Connection
from src.infrastructure.data.database import DatabaseConfiguration, Database
from src.infrastructure.data.database import MetaDatabaseBuilder
from src.infrastructure.data.config import pg_pg_login, pg_rx2m_login
from src.infrastructure.data.config import j2_rx2m_login
# --------------------------------------------------------------------------- #
logger = logging.getLogger(__name__)
# --------------------------------------------------------------------------- #


# @pytest.fixture(scope="class")
# def connection():
#     db = Database()
#     connection = Connection(pg_pg_login)
#     return connection


@pytest.fixture(scope="class")
def builder():
    """Builds the database builder configuration object."""
    filepath = "data/metadata/datasources.xlsx"
    df = pd.read_excel(filepath, index_col=0)
    config = DatabaseConfiguration(
        name="rx2m",
        schema="metabase",
        dba_pg_credentials=pg_pg_login,
        dba_db_credentials=pg_rx2m_login,
        user_db_credentials=j2_rx2m_login,
        create_table_ddl_filepath="src/infrastructure/data/ddl/metabase/metadata_table_create.sql",
        drop_table_ddl_filepath="src/infrastructure/data/ddl/metabase/metadata_table_drop.sql",
        table_data={'datasource': df},
        replace_if_exists=True
    )

    return MetaDatabaseBuilder(config)
