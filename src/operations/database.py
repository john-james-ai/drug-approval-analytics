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
# Modified : Saturday, July 31st 2021, 1:15:57 am                             #
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

from ..utils.logger import exception_handler
from .sqlgen import CreateUser, DropUser, UserExists, SQLCommand
from .sqlgen import TableExists, DropTable, ColumnExists
from .sqlgen import CreateDatabase, DropDatabase, DatabaseRename
from .sqlgen import GrantPrivileges, DatabaseExists, RevokePrivileges
from .sqlgen import Insert, Select, Update, Delete
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
#                      SQLALCHEMY DATABASE ENGINE                             #
# --------------------------------------------------------------------------- #
# --------------------------------------------------------------------------- #
#                       PSYCOPG2 DATABASE ENGINE                              #
# --------------------------------------------------------------------------- #


class Engine:
    """Executes a series of SQLCommand objects."""

    def __init__(self, credentials):
        self._credentials = credentials

    def execute(self, name: str, command) -> \
        Union[int, bool, str, float, datetime, list, pd.DataFrame,
              np.array]:
        """Executes a single sql command on the object."""

        con = self.get_connection()
        cursor = con.cursor()

        response = cursor.execute(command.cmd, command.params)
        command.executed = datetime.now()

        cursor.close()
        self.return_connection(con)

        logger.info(
            "{} was completed successfully."
            .format(command.description))

        return response

    def execute_all(self, name: str, command: list) -> None:
        """Executes a series of different SQL command within a transaction."""

        con = self.get_connection()
        cursor = con.cursor()

        responses = []

        for command in command:
            response = cursor.execute(command.cmd, command.params)
            command.executed = datetime.now()
            responses.append(response)

        cursor.close()
        self.return_connection(con)

        cmds = "\n".join([command.description for command in command])
        logger.info(
            "The following command completed successfully:\n    {}."
            .format(cmds))

        return responses

    def execute_many(self, name: str, command) -> None:
        """Executes a single SQL command on a multiple objects."""

        con = self.get_connection()
        cursor = con.cursor()

        response = cursor.executemany(command.cmd, command.params)
        command.executed = datetime.now()

        cursor.close()
        self.return_connection(con)

        logger.info(
            "{} was completed successfully."
            .format(command.description))

        return response

    def execute_query(self, name: str, command: SQLCommand) -> \
        Union[int, bool, str, float, datetime, list, pd.DataFrame,
              np.array]:
        """Executes a single sql command on the object."""

        con = self.get_connection()
        cursor = con.cursor()

        cursor.execute(command.cmd, command.params)
        response = cursor.fetchall()
        command.executed = datetime.now()

        cursor.close()
        self.return_connection(con)

        logger.info(
            "{} was completed successfully."
            .format(command.description))

        return response

    def get_connection(self):
        """Opens connection, sets isolation level, returns the connection."""

        ConnectionPool.initialize(self._credentials)
        connection = ConnectionPool.get_connection()
        connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return connection

    def return_connection(self, connection) -> None:
        """Commits and returns the connection to the pool."""

        connection.commit()
        ConnectionPool.return_connection(connection)

    def close_all_connections(self) -> None:
        ConnectionPool.close_all_connections()


# --------------------------------------------------------------------------- #
#                         DATABASE ADMINISTRATION                             #
# --------------------------------------------------------------------------- #
class DBAdmin:
    """Database administration

    Provides CRUD support for users, databases and tables.

    Arguments
        credentials (dict): Dictionary of authorizing credentials.

    """

    temp_database = 'tempdb'

    def __init__(self, credentials: dict) -> None:
        self._credentials = credentials
        self._engine = Engine(credentials)

    @exception_handler()
    def create_user(self, credentials: dict, create_db: bool = False) \
            -> None:
        query = CreateUser()
        command = query.build(credentials, create_db)
        return self._engine.execute(command.name, command)

    @exception_handler()
    def drop_user(self, name):
        query = DropUser()
        command = query.build(name)
        return self._engine.execute(name, command)

    @exception_handler()
    def user_exists(self, name) -> None:
        query = UserExists()
        command = query.build(name)
        return self._engine.execute_query(name, command)[0][0]

    @exception_handler()
    def create_database(self, name) -> None:
        query = CreateDatabase()
        command = query.build(name)
        self._engine.execute(name, command)

    @exception_handler()
    def database_exists(self, name) -> bool:

        query = DatabaseExists()
        command = query.build(name)
        return self._engine.execute_query(name, command)[0][0]

    @exception_handler()
    def drop_database(self, name) -> None:
        query = DropDatabase()
        command = query.build(name)
        self._engine.execute(name, command)

    @exception_handler()
    def rename_database(self, name: str, newname: str) -> None:
        query = DatabaseRename()
        command = query.build(name, newname)
        self._engine.execute(name, command)

    @exception_handler()
    def grant_database_privileges(self, name, user) -> None:
        query = GrantPrivileges()
        command = query.build(name, user)
        self._engine.execute(name, command)

    @exception_handler()
    def revoke_database_privileges(self, name, user) -> None:
        query = RevokePrivileges()
        command = query.build(name, user)
        self._engine.execute(name, command)

    @exception_handler()
    def backup_database(self, credentials: dict, filepath: str) -> None:

        command = ['pg_dump',
                   '--dbname=postgresql://{}:{}@{}:{}/{}'.format(
                       credentials['user'],
                       credentials['password'],
                       credentials['host'],
                       credentials['port'],
                       credentials['dbname']),
                   '-Fc -f',
                   filepath,
                   '-v']

        self._run_process(command)

        logger.info("Executed BACKUP on the {} database".format(
            credentials['dbname']))

    @exception_handler()
    def restore_database(self, credentials: dict, filepath: str) -> None:

        command = ['pg_restore',
                   '--no-owner',
                   '--dbname=postgresql://{}:{}@{}:{}/{}'.format(
                       credentials['user'],
                       credentials['password'],
                       credentials['host'],
                       credentials['port'],
                       credentials['dbname']),
                   '-v',
                   filepath]

        self._run_process(command)

        logger.info("Database restored from {}".format(filepath))

    @exception_handler()
    def load_database(self, credentials: dict, filepath: str) -> None:

        command = 'pg_restore --no-owner -d {} {}'.format(
            credentials['dbname'], filepath)

        self._run_process(command)

        logger.info("Executed RESTORE on the {} database".format(
            credentials['dbname']))

    @exception_handler()
    def _run_process(self, command):
        """Wrapper for Python subprocess commands."""

        process = Popen(command,  shell=True, stdin=PIPE,
                        stdout=PIPE, stderr=PIPE, encoding='utf8')

        result = process.communicate()[0]
        print("\n------------------")
        print(process.returncode)
        print("------------------")

        if int(process.returncode) != 0:
            raise Exception(result)

    @exception_handler()
    def create_table(self, name: str, command: SQLCommand) -> None:
        """Creates a table according to the schema defined in the file.

        Arguments
            name (str): The name for the table.
            command (SQLCommand): SQL command in SQLCommand format.

        """
        self._engine.execute(name, command)

    @exception_handler()
    def drop_table(self, name: str, cascade: bool = False) -> None:
        query = DropTable()
        command = query.build(name, cascade)
        return self._engine.execute(name, command)

    @exception_handler()
    def table_exists(self, name: str, schema: str = 'PUBLIC') -> None:
        query = TableExists()
        command = query.build(name, schema)
        return self._engine.execute_query(name, command)

    @exception_handler()
    def column_exists(self, name: str, table: str) -> None:
        """Adds one or more columns to a table.

        Arguments:
            name (str): Name of the column.
            table (str): Name of the table.
        """

        query = ColumnExists()
        command = query.build(name, table)
        return self._engine.execute_query(name, command)

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
