#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# =========================================================================== #
# Project  : Drug Approval Analytics                                          #
# Version  : 0.1.0                                                            #
# File     : \src\platform\database\context.py                                #
# Language : Python 3.9.5                                                     #
# --------------------------------------------------------------------------  #
# Author   : John James                                                       #
# Company  : nov8.ai                                                          #
# Email    : john.james@nov8.ai                                               #
# URL      : https://github.com/john-james-sf/drug-approval-analytics         #
# --------------------------------------------------------------------------  #
# Created  : Sunday, August 15th 2021, 2:20:27 am                             #
# Modified : Monday, August 16th 2021, 12:57:50 am                            #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #

from .connect import PGConnectionPool
from src.application.config import DBCredentials
from src.infrastructure.database.connect import Connection
from .access import PGDao
# --------------------------------------------------------------------------- #

# --------------------------------------------------------------------------- #
#                             CONTEXT                                         #
# --------------------------------------------------------------------------- #


class Context:
    """Database context class encapsulating database connection, and access."""

    def __init__(self, connection: Connection,
                 dao: PGDao) -> None:

        self._connection = connection
        self._dao = dao

    def begin_transaction(self):
        self._connection.begin_transaction()

    def open(self):
        self._connection.open()

    def close(self):
        self._connection.close()

    def rollback(self):
        self._connection.rollback()

    @property
    def dao(self):
        self._dao(connection=self._connection)
        return self_dao

    @property
    def dbname(self):
        return self._credentials.dbname
