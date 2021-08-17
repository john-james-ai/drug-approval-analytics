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
# Modified : Monday, August 16th 2021, 9:50:25 am                             #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
import os
import pytest
import pandas as pd

from src.infrastructure.data.context import PGDao
from src.infrastructure.data.admin import DBAdmin
from src.application.config import pg_login
# -----------------------------------------------------------------------------#


@pytest.mark.context
class BackoveryTests:

    def test_backup(self, build_test_table, build_test_database):
        filepath = "tests/data/backovery.dump"
        if os.path.exists(filepath):
            os.remove(filepath)
        dbname = "test"
        connection = build_test_database
        build_test_table
        access = PGDao()
        df = access.get(table="sources", connection=connection)
        assert isinstance(
            df, pd.DataFrame), "DAOError: Get didn't return a dataframe"
        assert df.shape[0] == 10, "DAOError: Dataframe has no rows"
        assert df.shape[1] == 14, "DAOError: Dataframe has no columns"
        admin = DBAdmin()
        admin.backup(credentials=pg_login,
                     dbname=dbname,
                     filepath=filepath)
        assert os.path.exists(filepath), "Backup failed."
        assert os.path.getsize(filepath) > 100, "Backup file too small."

    def test_recovery(self, drop_test_database, build_test_database):
        drop_test_database
        connection = build_test_database
        filepath = "tests/data/backovery.dump"
        dbname = "test"
        admin = DBAdmin()
        admin.restore(pg_login, dbname, filepath)
        access = PGDao()
        df = access.get(table="sources", connection=connection)
        assert isinstance(
            df, pd.DataFrame), "Restore failure. Get didn't return a dataframe"
        assert df.shape[0] == 10, "Restore failure.: Dataframe has no rows"
        assert df.shape[1] == 14, "Restore failure.: Dataframe has no columns"
