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
# Modified : Friday, August 13th 2021, 2:21:45 am                             #
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
class Command:
    """This class is used to execute commands against the database."""

    @exception_handler()
    def execute(self, sequel: Sequel, connection: PGConnectionFactory) -> int:
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
    def execute_ddl(self, sequel: Sequel, connection: PGConnectionFactory) -> int:
        """Processes SQL DDL commands from file."""

        with connection.cursor() as cursor:
            cursor.execute(open(sequel.params, "r").read())
        cursor.close()
        logger.info(sequel.description)

# --------------------------------------------------------------------------- #


class Admin(ABC):
    """Abstract base class for database administration classes."""

    def __init__(self):
        self._command = Command()

    @abstractmethod
    def create(self, name: str, connection: PGConnectionFactory, *args, **kwargs):
        pass

    @abstractmethod
    def exists(self, name: str, connection: PGConnectionFactory, *args, **kwargs):
        pass

    @abstractmethod
    def delete(self, name: str, connection: PGConnectionFactory):
        pass


# --------------------------------------------------------------------------- #
class Access(ABC):
    """Abstract base class for database access classes."""

    def __init__(self):
        self._command = Command()

    @abstractmethod
    def create(self, name: str, connection: PGConnectionFactory, *args, **kwargs):
        pass

    @abstractmethod
    def exists(self, name: str, connection: PGConnectionFactory, *args, **kwargs):
        pass

    @abstractmethod
    def read(self, name: str, connection: PGConnectionFactory, *args, **kwargs):
        pass

    @abstractmethod
    def update(self, name: str, connection: PGConnectionFactory, *args, **kwargs):
        pass

    @abstractmethod
    def delete(self, name: str, connection: PGConnectionFactory):
        pass
