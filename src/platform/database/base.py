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
# Modified : Tuesday, August 10th 2021, 2:38:42 am                            #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
"""Database abstract base class for administration and access modules."""
from abc import ABC, abstractmethod
import logging
from typing import Union

from .sequel import Sequel
from .connect import PGConnectionFactory
from ...utils.logger import exception_handler
# --------------------------------------------------------------------------- #
logger = logging.getLogger(__name__)


class Database(ABC):
    """Abstract base class for database administration and access classes."""

    def __init__(self, credentials: dict, autocommit: bool = True):
        self._credentials = credentials
        self._autocommit = autocommit
        self._connection = None
        self._cursor = None
        self.connect()

    @exception_handler()
    def connect(self) -> None:
        PGConnectionFactory.initialize(self._credentials)
        self._connection = PGConnectionFactory.get_connection()
        self._connection.set_session(autocommit=self._autocommit)

    @exception_handler()
    def connection(self):
        return self._connection

    @exception_handler()
    def cursor(self):
        return self._connection.cursor()

    @exception_handler()
    def commit(self) -> None:
        self._connection.commit()

    @exception_handler()
    def close(self, connection=None) -> None:
        if connection:
            PGConnectionFactory.return_connection(connection)
        else:
            PGConnectionFactory.return_connection(self._connection)
        self._connection = None

    @exception_handler()
    def close_all(self) -> None:
        PGConnectionFactory.close_all()
        self._connection = None

    @exception_handler()
    def _read(self, sequel: Sequel) -> int:
        if not self._connection:
            self._connect()
        self._cursor = self._connection.cursor()
        self._cursor.execute(sequel.cmd, sequel.params)
        response = self._cursor.fetchall()
        self._cursor.close()
        logger.info(sequel.description)
        return response

    @exception_handler()
    def _process_ddl(self, filepath: str) -> int:
        """Creates tables using SQL in filepath."""
        if not self._connection:
            self._connect()
        self._connection.set_session(autocommit=True)

        with self._connection.cursor() as cursor:
            cursor.execute(open(filepath, "r").read())
            response = cursor.rowcount

        cursor.close()
        self._connection.set_session(autocommit=self._autocommit)

        logger.info("Created {} tables.".format(response))
        return response

    @exception_handler()
    def _modify(self, sequel: Sequel) -> int:
        if not self._connection:
            self._connect()
        cursor = self._connection.cursor()
        cursor.execute(sequel.cmd, sequel.params)
        cursor.close()
        logger.info(sequel.description)

    @abstractmethod
    def create(self, *args, **kwargs) -> Union[any]:
        pass

    def read(self, *args, **kwargs) -> Union[any]:
        pass

    def update(self, *args, **kwargs) -> Union[any]:
        pass

    @abstractmethod
    def delete(self, *args, **kwargs) -> Union[any]:
        pass
