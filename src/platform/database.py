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
# Modified : Saturday, July 24th 2021, 6:14:03 am                             #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
"""Postgres database administration, access and connection pools. """
import logging
from typing import Union
from subprocess import Popen, PIPE
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2 import pool
import numpy as np
import pandas as pd
from datetime import datetime
from abc import ABC, abstractmethod

from ..utils.logger import exception_handler
from .querybuilder import TableExists
from .querybuilder import ColumnInserter, ColumnRemover
from .querybuilder import CreateTable, DropTable
from .querybuilder import DropDatabase, GrantPrivileges
from .querybuilder import CreateDatabase, DatabaseExists
# --------------------------------------------------------------------------- #
logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
#                              CONNECTION POOL                                #
# --------------------------------------------------------------------------- #


class ConnectionPool:
    """Creates and manages Postgres connection pools."""

    __connection_pool = None

    @staticmethod
    def initialize(credentials):
        ConnectionPool.__connection_pool = pool.SimpleConnectionPool(
            2, 10, **credentials)
        logger.info("Initialized connection pool for {} database.".format(
            credentials['dbname']))

    @staticmethod
    def get_connection():
        con = ConnectionPool.__connection_pool.getconn()
        name = con.info.dsn_parameters['dbname']
        logger.info(
            "Getting connection from {} connection pool.".format(name))
        return con

    @staticmethod
    def return_connection(connection):
        ConnectionPool.__connection_pool.putconn(connection)
        name = connection.info.dsn_parameters['dbname']
        logger.info(
            "Returning connection to {} connection pool.".format(name))

    @staticmethod
    def close_all_connections():
        ConnectionPool.__connection_pool.closeall()


# --------------------------------------------------------------------------- #
#                            DATABASE ENGINE                                  #
# --------------------------------------------------------------------------- #
class Engine:
    """Executes a series of SQLCommand objects."""

    def __init__(self, credentials: dict) -> None:
        self._credentials = credentials

    def execute(self, name: str, command) -> \
        Union[int, bool, str, float, datetime, list, pd.DataFrame,
              np.array]:
        """Executes a single sql command on the object."""

        cursor = self._bind_connection()

        cursor.execute(command.cmd, command.params)
        command.executed = datetime.now()

        self._release_connection(cursor)

        logger.info(
            "{} was completed successfully."
            .format(command.description))

    def execute_all(self, name: str, command: list) -> None:
        """Executes a series of different SQL command within a transaction."""

        cursor = self._bind_connection()

        for command in command:
            cursor.execute(command.cmd, command.params)
            command.executed = datetime.now()

        self._release_connection(cursor)

        cmds = "\n".join([command.description for command in command])
        logger.info(
            "The following command completed successfully:\n    {}."
            .format(cmds))

    def execute_many(self, name: str, command) -> None:
        """Executes a single SQL command on a multiple objects."""

        cursor = self._bind_connection()

        cursor.executemany(command.cmd, command.params)
        command.executed = datetime.now()

        self._release_connection(cursor)

        logger.info(
            "{} was completed successfully."
            .format(command.description))

    def execute_query(self, name: str, command) -> \
        Union[int, bool, str, float, datetime, list, pd.DataFrame,
              np.array]:
        """Executes a single sql command on the object."""

        cursor = self._bind_connection()

        cursor.execute(command.cmd, command.params)
        response = cursor.fetchone()[0]
        command.executed = datetime.now()

        self._release_connection(cursor)

        logger.info(
            "{} was completed successfully."
            .format(command.description))

        return response

    def _bind_connection(self):
        """Opens connection, sets isolation level, returns the connection."""

        ConnectionPool.initialize(self._credentials)
        self._connection = ConnectionPool.get_connection()
        self._connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = self._connection.cursor()
        return cursor

    def _release_connection(self, cursor) -> None:
        """Commits and returns the connection to the pool."""

        cursor.close()
        self._connection.commit()
        ConnectionPool.return_connection(self._connection)


# --------------------------------------------------------------------------- #
#                           ADMINISTRATION BASE                               #
# --------------------------------------------------------------------------- #
class Admin(ABC):
    """Base class for database and table administration."""

    def __init__(self, credentials: dict) -> None:
        self._credentials = credentials
        self._engine = Engine(credentials)

    @abstractmethod
    def create(self, name: str, *args, **kwargs) -> None:
        pass

    @abstractmethod
    def drop(self, name: str) -> None:
        pass

    @abstractmethod
    def exists(self, name: str) -> bool:
        pass

    def backup(self, name: str, filepath: str) -> None:
        pass

    def restore(self, name: str, filepath: str) -> None:
        pass


# --------------------------------------------------------------------------- #
#                         DATABASE ADMINISTRATION                             #
# --------------------------------------------------------------------------- #


class DBA(Admin):
    """Database administration i.e. creating and dropping databases.

    TODO: Backup and recovery
    """

    @exception_handler()
    def create(self, name) -> None:
        query = CreateDatabase()
        command = query.build(name)
        self._engine.execute(name, command)

    @exception_handler()
    def grant(self, name) -> None:
        query = GrantPrivileges(name, self._credentials)
        command = query.build(name)
        self._engine.execute(name, command)

    @exception_handler()
    def exists(self, name) -> None:
        query = DatabaseExists()
        command = query.build(name)
        return self._engine.execute_query(name, command)

    @exception_handler()
    def drop(self, name) -> None:
        query = DropDatabase()
        command = query.build(name)
        self._engine.execute(name, command)

    @exception_handler()
    def backup(self, name: str, filepath: str) -> None:

        command = Popen(
            ['pg_dump',
             '--dbname=postgresql://{}:{}@{}:{}/{}'
                .format(self._credentials['user'],
                        self._credentials['password'],
                        self._credentials['host'],
                        self._credentials['port'],
                        self._credentials['dbname']),
                '-Fc -f', filepath, '-v'],
            stdout=PIPE)

        result = command.communicate()[0]

        if int(command.returncode) != 0:
            raise Exception(command.returncode)

        # TODO: Write to repository.

        return result

    @exception_handler()
    def restore(self, name: str, filepath: str) -> None:

        command = Popen(
            ['pg_restore',
             '--no-owner',
             '--dbname=postgresql://{}:{}@{}:{}/{}'
                .format(self._credentials['user'],
                        self._credentials['password'],
                        self._credentials['host'],
                        self._credentials['port'],
                        self._credentials['dbname']),
                '-v', filepath],
            stdout=PIPE)

        result = command.communicate()[0]

        if int(command.returncode) != 0:
            raise Exception(command.returncode)

        # TODO: Write to repository.

        return result

# --------------------------------------------------------------------------- #
#                         TABLE ADMINISTRATION                                #
# --------------------------------------------------------------------------- #


class TableAdmin(Admin):
    """Database table administration."""

    @exception_handler()
    def create(self, name: str, schema: str, columns: list) -> None:
        """Creates a table according to the schema defined in the file.

        Arguments
            name (str): The name for the table.
            schema (list): list of strings defining column properties

        """
        query = CreateTable()
        command = query.build(name, schema, columns)
        self._engine.execute(name, command)

    @exception_handler()
    def drop(self, name: str, schema: str) -> None:
        query = DropTable()
        command = query.build(name, schema)
        self._engine.execute(name, command)

    @exception_handler()
    def exists(self, name: str, schema: str) -> None:
        query = TableExists()
        command = query.build(name, schema, self._credentials)
        return self._engine.execute_query(name, command)

    @exception_handler()
    def add_columns(self, name: str, schema: str,
                    columns: dict) -> None:
        """Adds one or more columns to a table.

        Arguments:
            name (str): the name of the table
            schema (list): List of dict objects.
        """

        query = ColumnInserter()
        command = query.build(name, schema, columns)
        self._engine.execute(name, command)

    @exception_handler()
    def drop_columns(self, name: str, schema: str, columns: list) -> None:
        """Drops one or more columns from a table.

        Arguments:
            name (str): the name of the table
            columns (list): a list of column names to drop.
        """

        query = ColumnRemover()
        command = query.build(name, schema)
        self._engine.execute(name, command)

# --------------------------------------------------------------------------- #
#                      DATABASE ACCESS OBJECT                                 #
# --------------------------------------------------------------------------- #
