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
# Created  : Saturday, August 14th 2021, 10:11:57 pm                          #
# Modified : Monday, August 16th 2021, 3:37:25 am                             #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
"""Database context class."""
from abc import ABC, abstractmethod
import logging
from typing import Union
import uuid

import pandas as pd

from .connect import Command
from .sequel import Sequel, AccessSequel
from src.application.config import DBCredentials
from ...utils.logger import exception_handler
# --------------------------------------------------------------------------- #
logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
#                      DATABASE ACCESS OBJECT                                 #
# --------------------------------------------------------------------------- #

class Access(ABC):
    """Abstract base class for database context classes."""

    def __init__(self, connection, end=0) -> None:
        self._connection = connection
        self._command = Command()

        self._end = end

    def __iter__(self):
        self._index - 0
        return self

    @abstractmethod
    def __next__(self):
        pass

    @abstractmethod
    def create(self, name: str, columns: list,
               values: list, schema: str = 'public') -> None:
        pass

    @abstractmethod
    def read(self, name: str, columns: list = None,
             filter_key: str = None,
             filter_value: Union[str, int, float] = None,
             schema: str = 'public')\
            -> pd.DataFrame:
        pass

    @abstractmethod
    def update(self, name: str, column: str,
               value: Union[str, float, int], filter_key: str,
               filter_value: Union[str, float, int],
               schema: str = 'public') -> None:
        pass

    @abstractmethod
    def delete(self, name: str, filter_key: str,
               filter_value: Union[str, float, int],
               schema: str = 'public') \
            -> None:
        pass


# --------------------------------------------------------------------------- #
class PGDao(Access):
    """Postgres data access object."""

    def __init__(self, connection, name=None) -> None:
        """Postgres Database Context Object (PGDao)

        Arguments:
            connection (psycopg2.connection): The Postgres database connection.
            name (str): A name of a table to which access is required.

        Dependencies:
            AccessSequel (Sequel): Serves parameterized SQL statements

        """
        super(PGDao, self).__init__(connection)
        self._name = name
        self._sequel = AccessSequel()
        self._response = None
        self._response_description = None

    @exception_handler()
    def __iter__(self):
        sequel = self._sequel.read(name=self._name, schema='public')
        self._response = self._command.execute_one(sequel, self._connection)
        try:
            self._columns = [element[0]
                             for element in self._response.description]
        except Exception as e:
            logger.error(e)
        return self

    @exception_handler()
    def __next__(self):
        if self._response.fetchone is not None:
            result = pd.DataFrame(
                data=list(self._response.fetchone), index=list(self._columns))
            self._response = self._command.execute_next(self._response.cursor)
            return result.T
        else:
            raise StopIteration

    @exception_handler()
    def create(self, name: str, columns: list,
               values: list, schema: str = 'public') -> None:
        """Adds a row to the designated table.

        Arguments

            name (str): Name of table
            columns (list): List of columns to retrieve.
                Optional default = all columns
            values (list): List of values corresponding with the columns.
            schema (str): The schema to which the table belongs.
                Optional. Default='public'

        Returns:
            rowcount (int): The number of rows inserted.
        """
        id = str(uuid.uuid4())
        columns.append('id')
        values.append(id)

        sequel = self._sequel.create(name=name, schema=schema,
                                     columns=columns, values=values)
        response = self._command.execute(sequel, self._connection)
        return response

    @exception_handler()
    def read(self, name: str, columns: list = None,
             filter_key: str = None,
             filter_value: Union[str, int, float] = None,
             schema: str = 'public')\
            -> pd.DataFrame:
        """Reads data from a table

        Arguments
            name (str): Table from which to read
            columns (list): List of columns to return. Optional
                if not provided, all columns will be returned.
            filter_key (str): Column containing value for subset
                Optional. If no value is provided, all rows
                are returned.
            filter_value (Union[str, int, float]) The value to match.
                Optional. If no value is provided, all rows
                are returned.

        """
        sequel = self._sequel.read(name=name, schema=schema,
                                   columns=columns, filter_key=filter_key,
                                   filter_value=filter_value)
        response = self._command.execute(sequel, self._connection)

        colnames = [element[0] for element in response.description]
        df = pd.DataFrame(data=response.fetchall, columns=colnames)

        return df

    @exception_handler()
    def update(self, name: str, column: str,
               value: Union[str, float, int], filter_key: str,
               filter_value: Union[str, float, int],
               schema: str = 'public') -> None:
        """Updates a row in the designated table.

        Arguments
            name (str): Name of table
            column (str): The column to update
            value (Union[str, float, int]): The value to assign to column.
            filter_key (str): Column upon which the condition applies.
            filter_value (Union[str, int, float]): Value to which
                filter_key must match.
            schema (str): The schema to which the table belongs.
                Optional. Default='public'

        Returns:
            rowcount (int): The number of rows updated.
        """
        sequel = self._sequel.update(name=name,
                                     schema=schema, column=column,
                                     value=value, filter_key=filter_key,
                                     filter_value=filter_value)

        response = self._command.execute(sequel, self._connection)

        return response

    @exception_handler()
    def delete(self, name: str, filter_key: str,
               filter_value: Union[str, float, int],
               schema: str = 'public') \
            -> None:
        """Deletes data from the designated table.

        Arguments
            name (str): Name of table
            filter_key (str): Column upon which the condition applies.
            filter_value (Union[str, int, float]): Value to which
                filter_key must match.
            schema (str): The schema to which the table belongs.
                Optional. Default='public'

        Returns:
            rowcount (int): The number of rows deleted.

        """
        sequel = self._sequel.delete(name=name, schema=schema,
                                     filter_key=filter_key,
                                     filter_value=filter_value)
        response = self._command.execute(sequel, self._connection)

        return response
