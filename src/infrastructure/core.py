#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# =========================================================================== #
# Project  : Drug Approval Analytics                                          #
# Version  : 0.1.0                                                            #
# File     : \src\platform\database\core.py                                   #
# Language : Python 3.9.5                                                     #
# --------------------------------------------------------------------------  #
# Author   : John James                                                       #
# Company  : nov8.ai                                                          #
# Email    : john.james@nov8.ai                                               #
# URL      : https://github.com/john-james-sf/drug-approval-analytics         #
# --------------------------------------------------------------------------  #
# Created  : Sunday, August 15th 2021, 2:30:11 am                             #
# Modified : Sunday, August 15th 2021, 2:31:03 am                             #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
"""Core modules used internally."""
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
import logging
from typing import Union

import psycopg2

from .sequel import Sequel

from .connect import PGConnectionFactory
from .config import DBCredentials
from ...utils.logger import exception_handler
# --------------------------------------------------------------------------- #
logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
class Response:
    """Class representing responses from database commands."""

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
