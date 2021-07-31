#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# =========================================================================== #
# Project  : Drug Approval Analytics                                          #
# Version  : 0.1.0                                                            #
# File     : \src\platform\database.py                                        #
# Language : Python 3.9.5                                                     #
# --------------------------------------------------------------------------  #
# Author   : John James                                                       #
# Company  : nov8.ai                                                          #
# Email    : john.james@nov8.ai                                               #
# URL      : https://github.com/john-james-sf/drug-approval-analytics         #
# --------------------------------------------------------------------------  #
# Created  : Friday, July 23rd 2021, 1:23:26 pm                               #
# Modified : Saturday, July 31st 2021, 11:16:25 am                            #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
"""Database Access Layer"""
import logging
from typing import Union
import numpy as np
import pandas as pd
from datetime import datetime

from ..utils.logger import exception_handler
from .database import Engine
from .sqlgen import SQLCommand
# --------------------------------------------------------------------------- #
logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
#                      DATABASE ACCESS OBJECT                                 #
# --------------------------------------------------------------------------- #


class DBAccess:
    """Database Access Object."""

    def __init__(self, credentials: dict) -> None:
        self._credentials = credentials
        self._engine = Engine(credentials)

    def _get_connection(self):
        """Returns a connection createe by the database engine."""
        return self._engine.get_connection()

    def _return_connection(self, connection):
        """Returns a connection back to the connection pool."""
        self._engine.return_connection(connection)

    def _execute(self, name: str, command: SQLCommand) -> Union[None, int]:
        self._engine.execute(name, command)

    def create(self, table: str, columns: list, values: list) -> None:
        """Inserts a row into the designated table

        Arguments
            table (str): Name of the table into which insertion occurs.
            columns (list): List of string names of columns
            values (list): List of values inserted into the above.
        """
        query = Insert()
        command = query.build(table, columns, values)
        return self._execute(table, command)

    def read(self, table: str, columns: list = None, where_key: str = None,
             where_value: Union[str, int, float] = None) -> pd.DataFrame:
        """Uses pandas to read data and return in DataFrame format.

        Arguments
            table (str): Name of the table into which insertion occurs.
            columns (list): The list of columns to read
            where_key (str): The name of the select column
            where_value  (str, int, float): The value for the select
                column.
        """
        query = Select()
        command = query.build(table, columns, where_key, where_value)

        con = self._get_connection()

        df = pd.read_sql(command.cmd, con)

        self._return_connection(con)

        return df

    def update(self, table: str, column: str, value: str, where_key: str,
               where_value=Union[str, float, int]) -> None:
        """Updates a row into the designated table where key = value.

        Arguments
            table (str): Name of the table into which insertion occurs.
            column (str): The name of the column to be updated.
            value (int, str, float): The value to be set
            where_key (str): The name of the select column
            where_value  (str, int, float): The value for the select
                column.

        """
        query = Update()
        command = query.build(table, column, value, where_key, where_value)
        self._execute(table, command)

    def delete(self, table: str, where_key: str,
               where_value=Union[str, float, int]) -> None:
        """Deletes a row from the designated table where key = value.

        Arguments
            table (str): Name of the table into which insertion occurs.
            where_key (str): The name of the select column
            where_value  (str, int, float): The value for the select
                column.

        """
        query = Delete()
        command = query.build(table, where_key, where_value)
        self._execute(table, command)
