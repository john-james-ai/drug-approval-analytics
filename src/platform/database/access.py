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
# Modified : Sunday, August 8th 2021, 1:19:20 pm                              #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
"""Database Access Object."""
from abc import ABC, abstractmethod
import logging
from typing import Union

import pandas as pd
import pandas.io.sql as sqlio
import psycopg2

from .sequel import AccessSequel
from ...utils.logger import exception_handler
# --------------------------------------------------------------------------- #
logger = logging.getLogger(__name__)


class DAO(ABC):
    """Abstract base class for database administration classes."""

    def __init__(self) -> None:
        self._sequel = AccessSequel()

    @abstractmethod
    def get(self, table: str, columns: list = None,
            where_key: str = None,
            where_value: Union[str, int, float] = None,
            schema: str = 'public',
            connection=None)\
            -> pd.DataFrame:
        """Selects data from the designated table.

        Arguments
            table (str): Name of table
            columns (list): List of columns to retrieve.
                Optional default = all columns
            where_key (str): Column upon which the condition applies.
                Optional. Default=None
            where_value (Union[str, int, float]): Value to which
                where_key must match. Optional. Default=None
            schema (str): The schema to which the table belongs.
                Optional. Default='public'
            connection (psycopg2.connection): The database connection.
                Optional. If not provided one will be made. This
                provides the client the option of performing
                several commands using the same connection.

        """
        pass

    @abstractmethod
    def add(self, table: str, columns: list,
            values: list,
            schema: str = 'public',
            connection=None) -> None:
        """Adds a row to the designated table.

        Arguments
            table (str): Name of table
            columns (list): List of columns to retrieve.
                Optional default = all columns
            schema (str): The schema to which the table belongs.
                Optional. Default='public'
            connection (psycopg2.connection): The database connection.
                Optional. If not provided one will be made. This
                provides the client the option of performing
                several commands using the same connection.

        """
        pass

    @abstractmethod
    def update(self, table: str, column: str,
               value: Union[str, float, int], where_key: str,
               where_value: Union[str, float, int],
               schema: str = 'public',
               connection=None) -> None:
        """Updates a row in the designated table.

        Arguments
            table (str): Name of table
            column (str): The column to update
            value (Union[str, float, int]): The value to assign to column.
            where_key (str): Column upon which the condition applies.
            where_value (Union[str, int, float]): Value to which
                where_key must match.
            schema (str): The schema to which the table belongs.
                Optional. Default='public'
            connection (psycopg2.connection): The database connection.
                Optional. If not provided one will be made. This
                provides the client the option of performing
                several commands using the same connection.

        """
        pass

    @abstractmethod
    def delete(self, table: str, where_key: str,
               where_value: Union[str, float, int],
               schema: str = 'public',
               connection=None) \
            -> None:
        """Deletes data from the designated table.

        Arguments
            table (str): Name of table
            where_key (str): Column upon which the condition applies.
            where_value (Union[str, int, float]): Value to which
                where_key must match.
            schema (str): The schema to which the table belongs.
                Optional. Default='public'
            connection (psycopg2.connection): The database connection.
                Optional. If not provided one will be made. This
                provides the client the option of performing
                several commands using the same connection.

        """
        pass
# --------------------------------------------------------------------------- #
#                      DATABASE ADMINISTRATION                                #
# --------------------------------------------------------------------------- #


class PGDao(DAO):
    """Postgres data access object."""

    @exception_handler()
    def get(self, connection, table: str, columns: list = None,
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

        cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute(sequel.cmd, sequel.params)
        data = cursor.fetchall()
        cursor.close()

        colnames = [element[0] for element in cursor.description]
        df = pd.DataFrame(data=data, columns=colnames)

        logger.info(sequel.description)

        return df

    @exception_handler()
    def add(self, connection, table: str, columns: list,
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
        """

        sequel = self._sequel.add(table=table, schema=schema,
                                  columns=columns, values=values)
        cursor = connection.cursor()
        cursor.execute(sequel.cmd, sequel.params)
        cursor.close()

        logger.info(sequel.description)

    @exception_handler()
    def update(self, connection, table: str, column: str,
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

        """
        sequel = self._sequel.update(table=table,
                                     schema=schema, column=column,
                                     value=value, where_key=where_key,
                                     where_value=where_value)
        cursor = connection.cursor()
        cursor.execute(sequel.cmd, sequel.params)
        cursor.close()

        logger.info(sequel.description)

    @exception_handler()
    def delete(self, connection, table: str, where_key: str,
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

        """
        sequel = self._sequel.delete(table=table, schema=schema,
                                     where_key=where_key,
                                     where_value=where_value)
        cursor = connection.cursor()
        cursor.execute(sequel.cmd, sequel.params)
        cursor.close()

        logger.info(sequel.description)
