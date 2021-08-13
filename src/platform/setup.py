#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# =========================================================================== #
# Project  : Drug Approval Analytics                                          #
# Version  : 0.1.0                                                            #
# File     : \src\platform\setup.py                                           #
# Language : Python 3.9.5                                                     #
# --------------------------------------------------------------------------  #
# Author   : John James                                                       #
# Company  : nov8.ai                                                          #
# Email    : john.james@nov8.ai                                               #
# URL      : https://github.com/john-james-sf/drug-approval-analytics         #
# --------------------------------------------------------------------------  #
# Created  : Friday, July 23rd 2021, 1:19:20 pm                               #
# Modified : Tuesday, August 10th 2021, 7:11:13 pm                            #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
"""Setup Operations databases."""
import logging

import pandas as pd

from .database.admin import DBAdmin, UserAdmin, TableAdmin

# --------------------------------------------------------------------------- #
logger = logging.getLogger(__name__)


class PlatformBuilder:
    """Builds the operations platform database

    Arguments:
        credentials (dict): Credentials for operations database
    """

    def __init__(self, credentials: DBCredentials) -> None:
        self._credentials = credentials
        self._dba = DBAdmin(credentials)
        self._ua = UserAdmin(credentials)
        self._ta = TableAdmin(credentials)

    def build_database(self, name):
        """Builds the Platform database"""

        if self._dba.exists(name):
            print("Database {} already exists".format(name))
            drop_recreate = input(
                "Would you like to drop and recreate the database? ['n']"
            )
            if drop_recreate in ['y', 'Y', 'yes', 'YES']:
                self._dba.delete(name)
                self._dba.create(name)
        else:
            self._dba.create(name)
        logger.info("Database {} created.".format(name))

    def build_user(self, credentials) -> None:
        """Builds the database user with createdb privileges."""
        print(credentials)
        if self._ua.exists(credentials['user']):
            print("User {} already exists.".format(credentials['user']))
            drop_recreate = input(
                "Would you like to drop and recreate the user? ['n']")
            if drop_recreate in ['y', 'Y', 'yes', 'YES']:
                self._ua.drop(credentials['user'])
                self._ua.create(name=credentials['user'],
                                credentials=credentials, create_db=True)
        else:
            self._ua.create(name=credentials['user'],
                            credentials=self._credentials, create_db=True)

        self._credentials = credentials
        self._dba = DBAdmin(credentials)
        self._ua = UserAdmin(credentials)
        self._ta = TableAdmin(credentials)
        logger.info("User {} registered with createdb privileges.")

    def build_metadata(self, filepath):
        """Creates metadata tables."""
        self._dba.create(filepath)
        logger.info("Metadata tables developed.")

    def build_datasources(self, filepath):
        df = pd.DataFrame(filepath, indexcol=0)
        self._dba.load(name='datasources', data=df)


# %%
