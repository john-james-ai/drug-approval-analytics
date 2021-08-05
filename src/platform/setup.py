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
# Modified : Wednesday, August 4th 2021, 11:05:16 pm                          #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
"""Setup Operations databases."""

from .sqllib import operations_tables
from .database import DBAdmin, UserAdmin, TableAdmin
from .config import dba_credentials, operator_credentials
from .sqlgen import Sequel
# --------------------------------------------------------------------------- #


class OperationsBuilder:
    """Builds the operations platform database

    Arguments:
        credentials (dict): Credentials for operations database
    """

    def __init__(self, credentials: dict, tables: dict) -> None:
        self._credentials = credentials
        self._tables = tables

    def build_database(self, name):
        """Builds the Operations database"""
        dba = DBAdmin(self._credentials)

        if dba.exists(name):
            print("Database {} already exists".format(name))
            drop_recreate = input(
                "Would you like to drop and recreate the database? ['n']"
            )
            if drop_recreate in ['y', 'Y', 'yes', 'YES']:
                dba.drop(name)
                dba.create(name)
        else:
            dba.create(name)

    def build_user(self, credentials) -> None:
        """Builds the database user with createdb privileges."""

        ua = UserAdmin(self._credentials)

        if ua.exists(credentials.user):
            print("User {} already exists.".format(credentials.user))
            drop_recreate = input(
                "Would you like to drop and recreate the user? ['n']")
            if drop_recreate in ['y', 'Y', 'yes', 'YES']:
                ua.drop(credentials.user)
                ua.create(name=credentials.user,
                          credentials=credentials, create_db=True)
        else:
            ua.create(name=credentials.user,
                      credentials=self._credentials, create_db=True)

    def build_tables(self):
        """Creates operations tables."""
        ta = TableAdmin(self._credentials)
        for name, command in self._tables.items():

            # Generate the Sequel object for the table.
            sql_cmd = Sequel(name=name,
                             description="Create {} table.".format(name),
                             cmd=command)

            if ta.exists(name):
                print("Table {} already exists".format(name))
                drop_recreate = input(
                    "Would you like to drop and recreate the table? ['n']"
                )
                if drop_recreate in ['y', 'Y', 'yes', 'YES']:
                    ta.drop(name)
                    ta.create(name, sql_cmd)
            else:
                ta.create(name, sql_cmd)


def main():
    builder = OperationsBuilder(operator_credentials, operations_tables)
    builder.build_user()
    builder.build_database()
    builder.build_tables()


if __name__ == '__main__':
    main()
# %%
