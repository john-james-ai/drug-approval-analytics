#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# =========================================================================== #
# Project  : Drug Approval Analytics                                          #
# Version  : 0.1.0                                                            #
# File     : \tests\test_data\test_datasource.py                              #
# Language : Python 3.9.5                                                     #
# --------------------------------------------------------------------------  #
# Author   : John James                                                       #
# Company  : nov8.ai                                                          #
# Email    : john.james@nov8.ai                                               #
# URL      : https://github.com/john-james-sf/drug-approval-analytics         #
# --------------------------------------------------------------------------  #
# Created  : Tuesday, August 10th 2021, 11:38:31 pm                           #
# Modified : Friday, August 13th 2021, 7:22:12 am                             #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
import pytest
import logging

from src.data.extract import Studies
from src.platform.metadata.repository import DataSource
from src.platform.config import rx2m_login

from tests.test_utils.debugging import announce
# --------------------------------------------------------------------------- #
logger = logging.getLogger(__name__)


@pytest.mark.habitue
class HabitueTests:

    @announce
    @pytest.mark.habitue
    def test_studies(self, build_test_metadata_database):
        build_test_metadata_database
        # Confirm database is setup
        sources = DataSource(rx2m_login)
        df = sources.read()
        assert df.shape[0] == 10, print(df)
        assert df.shape[1] == 25, print(df)

        habitue = Studies(rx2m_login)
        habitue.execute()
        logger.debug("\n\n\tTest Studies: Executed Habitue")

        # Confirm database is setup
        # df2 = sources.read("studies")
        # logger.debug("\n\n\tTest Studies: Read Studies")
        # assert df2.iloc[0]['has_changed'], print(df2.iloc[0]['has_changed'])

    def test_tear_down(self, tear_down_test_metadata_database):
        tear_down_test_metadata_database
