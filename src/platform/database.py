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
# Modified : Friday, July 23rd 2021, 9:43:20 pm                               #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
"""Postgres database administration, access and connection pools. """
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
import pandas as pd
import psycopg2
from psycopg2 import pool, sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from subprocess import Popen, PIPE
# import shlex

from ...utils.logger import exception_handler, logger
from querybuilder import CreateDatabase, CreateTable
from querybuilder import DropDatabase, DropTable, ColumnInserter, ColumnRemover
from querybuilder import SimpleQuery

# --------------------------------------------------------------------------- #
#                              SQL COMMAND                                    #
# --------------------------------------------------------------------------- #


@dataclass
class SQLCommand:
    """Class that encapsulates a sql command, its name and parameters."""
    name: str
    cmd: psycopg2.sql
    description: field(default=None)
    params: tuple = field(default_factory=tuple)
    executed: datetime = field(default=datetime(1970, 1, 1, 0, 0))

# --------------------------------------------------------------------------- #
#                              CONNECTION POOL                                #
# --------------------------------------------------------------------------- #


class ConnectionPool:
    """Creates and manages Postgres connection pools."""

    __connection_pool = None

    @exception_handler
    @staticmethod
    def initialise(credentials):
        ConnectionPool.__connection_pool = pool.SimpleConnectionPool(
            2, 10, **credentials)
        logger.info("Initialized connection pool for {} database.".format(
            credentials['name']))

    @exception_handler
    @staticmethod
    def get_connection():
        con = ConnectionPool.__connection_pool.getconn()
        name = con.info.dsn_parameters['name']
        logger.info(
            "Getting connection from {} connection pool.".format(name))
        return con

    @exception_handler
    @staticmethod
    def return_connection(connection):
        ConnectionPool.__connection_pool.putconn(connection)
        name = connection.info.dsn_parameters['name']
        logger.info(
            "Returning connection to {} connection pool.".format(name))

    @exception_handler
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
        self._connection_pool = ConnectionPool()

    def execute(self, name: str, command: SQLCommand) -> \
        Union[int, bool, str, float, datetime, list, pd.DataFrame,
              np.array]:
        """Executes a single sql command on the object."""

        connection = self._get_connection()
        cursor = connection.cursor()

        response = cursor.execute(command.cmd, command.params)
        command.executed = datetime.now()

        cursor.close()
        self._close_connection(connection)

        cmds = "\n".join([command.description for command in commands])
        logger.info(
            "The following commands completed successfully:\n    {}."
            .format(cmds))

        return response

    def execute_all(self, name: str, commands: list) -> None:
        """Executes a series of different SQL commands within a transaction."""

        connection = self._get_connection()
        cursor = connection.cursor()

        for command in commands:
            cursor.execute(command.cmd, command.params)
            command.executed = datetime.now()

        cursor.close()
        self._close_connection(connection)

        cmds = "\n".join([command.description for command in commands])
        logger.info(
            "The following commands completed successfully:\n    {}."
            .format(cmds))

    def execute_many(self, name: str, command: SQLCommand) -> None:
        """Executes a single SQL command on a multiple objects."""

        connection = self._get_connection()
        cursor = connection.cursor()

        cursor.executemany(command.cmd, command.params)
        command.executed = datetime.now()

        cursor.close()
        self._close_connection(connection)

        cmds = "\n".join([command.params for item in items])
        logger.info(
            "Successful completion of {} the following items:\n    {}."
            .format(cmds))

    def _get_connection(self) -> psycopg2.connection:
        """Opens connection, sets isolation level, returns the connection."""

        self._connection_pool.initialize(self._credentials)
        connection = self._connection_pool.get_connection()
        connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return connection

    def _close_connection(self, connection: psycopg2.connection) -> None:
        """Commits and returns the connection to the pool."""

        connection.commit()
        self._connection_pool.return_connection()(connection)


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

# --------------------------------------------------------------------------- #
#                         DATABASE ADMINISTRATION                             #
# --------------------------------------------------------------------------- #


class DBA(Admin):
    """Database administration i.e. creating and dropping databases.

    TODO: Backup and recovery
    """

    @exception_handler
    def create(self, name) -> None:
        query = CreateDatabase()
        commands = query.build(name, self._credentials)
        self._engine.execute(name, commands)

    @exception_handler
    def drop(self, name) -> None:
        query = DropDatabase()
        commands = query.build(name, self._credentials)
        self._engine.execute(name, commands)

# --------------------------------------------------------------------------- #
#                         TABLE ADMINISTRATION                                #
# --------------------------------------------------------------------------- #


class TableAdmin(Admin):
    """Database table administration."""

    @exception_handler
    def create(self, name: str, schema: str, columns: dict) -> None:
        """Creates a table according to the schema defined in the file.

        Arguments
            name (str): The name for the table.
            schema (list): List of dict objects

        """
        query = CreateTable()
        commands = query.build(name, schema)
        self._engine.execute(name, commands)

    @exception_handler
    def drop(self, name, schema: str) -> None:
        query = DropTable()
        commands = query.build(name, schema)
        self._engine.execute(name, commands)

    @exception_handler
    def exists(self, name: str, schema: str) -> bool:

        # Obtain the query from the query builder
        query = TableExists()
        command = query.build(name, schema)

        # Connect to the database and return a cursor
        connection = self._get_connection()
        cursor = connection.cursor()

        cursor.execute(command.sql, command.params)
        command.executed = datetime.now()

        cursor.close()
        self._close_connection(connection)

        cmds = "\n".join([command.description for command in commands])
        logger.info(
            "The following commands completed successfully:\n    {}."
            .format(cmds))

    @exception_handler
    def add_columns(self, name: str, schema: str,
                    columns: dict) -> None:
        """Adds one or more columns to a table.

        Arguments:
            name (str): the name of the table
            schema (list): List of dict objects.
        """

        query = ColumnInserter()
        commands = query.build(name, schema, columns)
        self._engine.execute(name, commands)

    @exception_handler
    def drop_columns(self, name: str, schema: str, columns: list) -> None:
        """Drops one or more columns from a table.

        Arguments:
            name (str): the name of the table
            columns (list): a list of column names to drop.
        """

        query = ColumnRemover()
        commands = query.build(name, schema)
        self._engine.execute(name, commands)

# --------------------------------------------------------------------------- #
#                      DATABASE ACCESS OBJECT                                 #
# --------------------------------------------------------------------------- #
