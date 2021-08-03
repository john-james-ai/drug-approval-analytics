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
# Modified : Tuesday, August 3rd 2021, 7:25:19 pm                             #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
"""Encapsulates database access object.

Contains CRUD SQL builder classes for and a database access object.
Two sets of classes are included: sql builder classes, and access classes
that engage the appropriate builder class.

Classes:
    Contains the following squl builder classes:
        - QueryBuilder: Base class
        - Insert: Generates SQL for Insert command
        - Select: Generates SQL for Select command
        - Update: Generates SQL for Update command
        - Delete: Generates SQL for Delete command

    Also includes PGAccess, the data access object class.

"""
from abc import ABC, abstractmethod
import logging
from typing import Union
from psycopg2 import sql
import pandas as pd

from ..base import Access
from ..utils.logger import exception_handler
# --------------------------------------------------------------------------- #
logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
#                             QUERY BUILDER                                   #
# --------------------------------------------------------------------------- #
class QueryBuilder(ABC):
    """Abstract base class for query builder classes."""

    @abstractmethod
    def build(self, *args, **kwargs):
        pass


class Insert(QueryBuilder):
    """Generates SQL for Insert commands"""

    @exception_handler()
    def build(self, table: str, columns: list, values: list) -> sql.SQL:

        if (len(columns) != len(values)):
            raise ValueError(
                "Number of columns doesn't match number of values")

        command = sql.SQL("INSERT into {} ({}) values {} RETURNING id;").\
            format(
            sql.Identifier(table),
            sql.SQL(', ').join(map(sql.Identifier, tuple((*columns,)))),
            sql.SQL(', ').join(sql.Placeholder() * len(columns))
        )

        return command


# --------------------------------------------------------------------------- #
class Select(QueryBuilder):
    """Generates SQL to support basic queries."""

    def _validate(self, table: str, columns: list = None,
                  where_key: str = None,
                  where_value: Union[str, int, float] = None) -> sql.SQL:

        if (where_key and where_value) !=\
                (where_key or where_value):
            raise ValueError("where values not completely specified.")

    def _build_cmd(self, table: str, columns: list = None) -> sql.SQL:

        return sql.SQL("SELECT {} FROM {}").format(
            sql.SQL(", ").join(map(sql.Identifier, columns)),
            sql.Identifier(table))

    def _build_where_cmd(self, table: str, columns: list = None,
                         where_key: str = None,
                         where_value: Union[str, int, float] = None)\
            -> sql.SQL:

        return sql.SQL("SELECT {} FROM {} WHERE {} = {} ").format(
            sql.SQL(", ").join(map(sql.Identifier, columns)),
            sql.Identifier(table),
            sql.Identifier(where_key),
            sql.Identifier(where_value))

    def build(self, table: str, columns: list = None, where_key: str = None,
              where_value: Union[str, int, float] = None) -> sql.SQL:

        self._validate(table, columns, where_key, where_value)

        columns = columns if columns is not None else ['*']

        if where_key and where_value:
            command = self._build_where_cmd(
                table, columns, where_key, where_value)
        else:
            command = self._build_cmd(table, columns)

        return command


# --------------------------------------------------------------------------- #
class Update(QueryBuilder):
    """Generates SQL to support single value update commands."""

    def build(self, table: str, column: str, value: Union[str, float, int],
              where_key: str,
              where_value=Union[str, float, int]) -> sql.SQL:

        command = sql.SQL("UPDATE {} SET {} = {} WHERE {} = {}").format(
            sql.Identifier(table),
            sql.Identifier(column),
            sql.Placeholder(),
            sql.Identifier(where_key),
            sql.Placeholder()
        )

        return command


# --------------------------------------------------------------------------- #
class Delete(QueryBuilder):
    """Generates SQL to support basic delete commands."""

    def build(self, table: str, where_key: str,
              where_value=Union[str, float, int]) -> sql.SQL:

        command = sql.SQL("DELETE FROM {} WHERE {} = {}").format(
            sql.Identifier(table),
            sql.Identifier(where_key),
            sql.Placeholder()
        )

        return command
# --------------------------------------------------------------------------- #
#                     POSTGRES DATABASE ACCESS OBJECT                         #
# --------------------------------------------------------------------------- #


class PGAccess(Access):
    """Data access object for Postgres database."""

    @ exception_handler()
    def create(self, connection, table: str, columns: list,
               values: list) -> int:
        """Inserts a row into a given table using the connection parameter.

        Arguments
            connection (psycopg2.connection): A connection to the database
            table (str): String containing the name of the table
            columns (list): List of column names to be inserted
            values (list): The corresponding values for the columns.

        """
        command = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            sql.Identifier(table),
            sql.SQL(",").join(map(sql.Identifier, columns)),
            sql.SQL(",").join(map(sql.Placeholder, columns))
        )

        query = Insert()
        command = query.build(table, columns, values)

        cursor = connection.cursor()

        cursor.execute(command, tuple((values,)))
        row_id = cursor.fetchone()[0]

        cursor.close()

        logger.info("Insert into {} table successful.".format(table))

        return row_id

    @ exception_handler()
    def read(self,  connection, table: str,
             columns: list = None, where_key: str = None,
             where_value: Union[str, int, float] = None) -> pd.DataFrame:
        """Uses pandas to read data and return in DataFrame format.

        Arguments
            connection (psycopg2.connection): A connection to the database
            table (str): Name of the table into which insertion occurs.
            columns (list): The list of columns to read
            where_key (str): The name of the select column
            where_value  (str, int, float): The value for the select
                column.
        """
        query = sql.Select()
        command = query.build(table, columns, where_key, where_value)

        df = pd.read_sql(command, connection)

        logger.info("Select from {} table successful.".format(table))

        return df

    @ exception_handler()
    def update(self, connection, table: str, column: str,
               value: str, where_key: str,
               where_value=Union[str, float, int]) -> None:
        """Updates a row into the designated table where key = value.

        Arguments
            connection (psycopg2.connection): A connection to the database
            table (str): Name of the table into which insertion occurs.
            column (str): The name of the column to be updated.
            value (int, str, float): The value to be set
            where_key (str): The name of the select column
            where_value  (str, int, float): The value for the select
                column.

        """
        query = Update()
        command = query.build(table, column, value, where_key, where_value)

        cursor = connection.cursor()

        cursor.execute(command, tuple((column, value,)))

        cursor.close()

        logger.info("Update column {} of {} table successful."
                    .format(column, table))

    def delete(self, connection, table: str, where_key: str,
               where_value=Union[str, float, int]) -> None:
        """Deletes a row from the designated table where key = value.

        Arguments
            connection (psycopg2.connection): A connection to the database
            table (str): Name of the table into which insertion occurs.
            where_key (str): The name of the select column
            where_value  (str, int, float): The value for the select
                column.

        """
        query = Delete()
        command = query.build(table, where_key, where_value)

        cursor = connection.cursor()

        cursor.execute(command)

        cursor.close()

        logger.info("Delete from {} table successful."
                    .format(table))
