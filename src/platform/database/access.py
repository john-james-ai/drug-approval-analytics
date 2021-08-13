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
# Modified : Thursday, August 12th 2021, 9:43:21 pm                           #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
"""Database Access Object."""
import logging
from typing import Union
import uuid

import pandas as pd

from .sequel import AccessSequel
from .base import Database
from ..config import DBCredentials
from ...utils.logger import exception_handler
# --------------------------------------------------------------------------- #
logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
#                      DATABASE ACCESS OBJECT                                 #
# --------------------------------------------------------------------------- #


class PGDao(Database):
    """Postgres data access object."""

    def __init__(self):
        """Postgres Database Access Object (PGDao)

        CRUD on table contents.

        Dependencies:
            AccessSequel (Sequel): Serves parameterized SQL statements

        """
        self._sequel = AccessSequel()

    @exception_handler()
    def create(self, table: str, columns: list,
               values: list, connection,
               schema: str = 'public') -> None:
        """Adds a row to the designated table.

        Arguments

            table (str): Name of table
            columns (list): List of columns to retrieve.
                Optional default = all columns
            values (list): List of values corresponding with the columns.
            connection (psycopg2.connection): The Postgres database connection.
            schema (str): The schema to which the table belongs.
                Optional. Default='public'

        Returns:
            rowcount (int): The number of rows inserted.
        """
        id = str(uuid.uuid4())
        columns.append('id')
        values.append(id)

        sequel = self._sequel.create_table(table=table, schema=schema,
                                           columns=columns, values=values)
        response = self._execute(sequel, connection)
        return response

    @exception_handler()
    def read(self, table: str, connection, columns: list = None,
             where_key: str = None,
             where_value: Union[str, int, float] = None,
             schema: str = 'public')\
            -> pd.DataFrame:
        """Reads data from a table

        Arguments
            table (str): Table from which to read
            connection (psycopg2.connection): The Postgres database connection.
            columns (list): List of columns to return. Optional
                if not provided, all columns will be returned.
            where_key (str): Column containing value for subset
                Optional. If no value is provided, all rows
                are returned.
            where_value (Union[str, int, float]) The value to match.
                Optional. If no value is provided, all rows
                are returned.

        """
        sequel = self._sequel.read(table=table, schema=schema,
                                   columns=columns, where_key=where_key,
                                   where_value=where_value)
        response = self._execute(sequel, connection)

        colnames = [element[0] for element in response.description]
        df = pd.DataFrame(data=response.fetchall, columns=colnames)

        return df

    @exception_handler()
    def update(self, table: str, column: str,
               value: Union[str, float, int], where_key: str,
               where_value: Union[str, float, int], connection,
               schema: str = 'public') -> None:
        """Updates a row in the designated table.

        Arguments            
            table (str): Name of table
            column (str): The column to update
            value (Union[str, float, int]): The value to assign to column.
            where_key (str): Column upon which the condition applies.
            where_value (Union[str, int, float]): Value to which
                where_key must match.
            connection (psycopg2.connection): The Postgres database connection.
            schema (str): The schema to which the table belongs.
                Optional. Default='public'

        Returns:
            rowcount (int): The number of rows updated.
        """
        sequel = self._sequel.update(table=table,
                                     schema=schema, column=column,
                                     value=value, where_key=where_key,
                                     where_value=where_value)

        response = self._execute(sequel, connection)

        return response

    @exception_handler()
    def delete(self, table: str, where_key: str,
               where_value: Union[str, float, int],
               connection,
               schema: str = 'public') \
            -> None:
        """Deletes data from the designated table.

        Arguments
            table (str): Name of table
            where_key (str): Column upon which the condition applies.
            where_value (Union[str, int, float]): Value to which
                where_key must match.
            connection (psycopg2.connection): The Postgres database connection.
            schema (str): The schema to which the table belongs.
                Optional. Default='public'

        Returns:
            rowcount (int): The number of rows deleted.

        """
        sequel = self._sequel.delete(table=table, schema=schema,
                                     where_key=where_key,
                                     where_value=where_value)
        response = self._execute(sequel, connection)

        return response
