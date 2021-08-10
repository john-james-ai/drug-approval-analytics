#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# =========================================================================== #
# Project  : Drug Approval Analytics                                          #
# Version  : 0.1.0                                                            #
# File     : \src\platform\data\access.py                                     #
# Language : Python 3.9.5                                                     #
# --------------------------------------------------------------------------  #
# Author   : John James                                                       #
# Company  : nov8.ai                                                          #
# Email    : john.james@nov8.ai                                               #
# URL      : https://github.com/john-james-sf/drug-approval-analytics         #
# --------------------------------------------------------------------------  #
# Created  : Tuesday, August 3rd 2021, 5:03:11 am                             #
# Modified : Tuesday, August 10th 2021, 1:03:59 am                            #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
"""Database Access Object."""
import logging
from typing import Union

import pandas as pd

from .sequel import AccessSequel
from .base import Database
from ...utils.logger import exception_handler
# --------------------------------------------------------------------------- #
logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
#                      DATABASE ACCESS OBJECT                                 #
# --------------------------------------------------------------------------- #


class PGDao(Database):
    """Postgres data access object."""

    def __init__(self, credentials: dict, autocommit: bool = True):
        super(PGDao, self).__init__(credentials=credentials,
                                    autocommit=autocommit)
        self._sequel = AccessSequel()

    @exception_handler()
    def create(self, table: str, columns: list,
               values: list,
               schema: str = 'public') -> None:
        """Adds a row to the designated table.

        Arguments
            connection (psycopg2.connection): The database connection.
            table (str): Name of table
            columns (list): List of columns to retrieve.
                Optional default = all columns
            schema (str): The schema to which the table belongs.
                Optional. Default='public'

        Returns:
            rowcount (int): The number of rows inserted.
        """

        sequel = self._sequel.add(table=table, schema=schema,
                                  columns=columns, values=values)
        self._modify(sequel)
        response = self._cursor.rowcount
        return response

    @exception_handler()
    def read(self, table: str, columns: list = None,
             where_key: str = None,
             where_value: Union[str, int, float] = None,
             schema: str = 'public')\
            -> pd.DataFrame:
        """Reads data from a table

        Arguments
            connection (psycopg2.connection): Connection to postgres
                database.
            table (str): Table from which to read
            columns (list): List of columns to return. Optional
                if not provided, all columns will be returned.
            where_key (str): Column containing value for subset
                Optional. If no value is provided, all rows
                are returned.
            where_value (Union[str, int, float]) The value to match.
                Optional. If no value is provided, all rows
                are returned.

        """
        sequel = self._sequel.get(table=table, schema=schema,
                                  columns=columns, where_key=where_key,
                                  where_value=where_value)
        response = self._read(sequel)

        colnames = [element[0] for element in self._cursor.description]
        df = pd.DataFrame(data=response, columns=colnames)

        return df

    @exception_handler()
    def update(self, table: str, column: str,
               value: Union[str, float, int], where_key: str,
               where_value: Union[str, float, int],
               schema: str = 'public') -> None:
        """Updates a row in the designated table.

        Arguments
            connection (psycopg2.connection): The database connection.
            table (str): Name of table
            column (str): The column to update
            value (Union[str, float, int]): The value to assign to column.
            where_key (str): Column upon which the condition applies.
            where_value (Union[str, int, float]): Value to which
                where_key must match.
            schema (str): The schema to which the table belongs.
                Optional. Default='public'

        Returns:
            rowcount (int): The number of rows updated.
        """
        sequel = self._sequel.update(table=table,
                                     schema=schema, column=column,
                                     value=value, where_key=where_key,
                                     where_value=where_value)
        self._modify(sequel)
        response = self._cursor.rowcount
        return response

    @exception_handler()
    def delete(self, table: str, where_key: str,
               where_value: Union[str, float, int],
               schema: str = 'public') \
            -> None:
        """Deletes data from the designated table.

        Arguments
            connection (psycopg2.connection): The database connection.
            table (str): Name of table
            where_key (str): Column upon which the condition applies.
            where_value (Union[str, int, float]): Value to which
                where_key must match.
            schema (str): The schema to which the table belongs.
                Optional. Default='public'

        Returns:
            rowcount (int): The number of rows deleted.

        """
        sequel = self._sequel.delete(table=table, schema=schema,
                                     where_key=where_key,
                                     where_value=where_value)
        self._modify(sequel)
        response = self._cursor.rowcount

        return response
