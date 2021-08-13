#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# =========================================================================== #
# Project  : Drug Approval Analytics                                          #
# Version  : 0.1.0                                                            #
# File     : \src\platform\database\base.py                                   #
# Language : Python 3.9.5                                                     #
# --------------------------------------------------------------------------  #
# Author   : John James                                                       #
# Company  : nov8.ai                                                          #
# Email    : john.james@nov8.ai                                               #
# URL      : https://github.com/john-james-sf/drug-approval-analytics         #
# --------------------------------------------------------------------------  #
# Created  : Monday, August 9th 2021, 11:44:10 pm                             #
# Modified : Thursday, August 12th 2021, 9:23:28 pm                           #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
"""Database abstract base class for administration and access modules."""
from abc import ABC, abstractmethod
import logging
from typing import Union

import psycopg2

from .sequel import Sequel
from .connect import PGConnectionFactory
from ...utils.logger import exception_handler
from ..config import DBCredentials
# --------------------------------------------------------------------------- #
logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
class Response:
    """Contains response data from database commands."""

    def __init__(self, cursor=None, fetchone=None, fetchall=None,
                 description=None, rowcount: int = 0):
        self.cursor = cursor
        self.fetchall = fetchall
        self.rowcount = rowcount
        self.description = description


# --------------------------------------------------------------------------- #
class Database(ABC):
    """Abstract base class for database administration and access classes."""

    @exception_handler()
    def _execute(self, sequel: Sequel, connection) -> int:
        cursor = connection.cursor()
        response_cursor = cursor.execute(sequel.cmd, sequel.params)
        response_description = cursor.description
        response_rowcount = cursor.rowcount

        try:
            response_fetchall = cursor.fetchall()
        except psycopg2.ProgrammingError:
            response_fetchall = None

        response = Response(cursor=response_cursor,
                            fetchall=response_fetchall,
                            description=response_description,
                            rowcount=response_rowcount)
        cursor.close()
        logger.info(sequel.description)
        return response

    @exception_handler()
    def _execute_ddl(self, sequel: Sequel, connection) -> int:
        """Processes SQL DDL commands from file."""

        with connection.cursor() as cursor:
            cursor.execute(open(sequel.params, "r").read())
        cursor.close()
        logger.info(sequel.description)
