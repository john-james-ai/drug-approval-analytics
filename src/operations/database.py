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
# Modified : Thursday, July 29th 2021, 7:46:04 pm                             #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
"""Postgres database administration, access and connection pools. """
from abc import ABC, abstractmethod
import logging
from typing import Union
from subprocess import Popen, PIPE
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2 import pool
import numpy as np
import pandas as pd
from datetime import datetime


from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base, ConcreteBase
from sqlalchemy.orm import sessionmaker

from ..utils.logger import exception_handler
from .sqlgen import CreateUser, DropUser, UserExists, SQLCommand
from .sqlgen import ColumnInserter, ColumnRemover
from .sqlgen import TableExists, DropTable, ColumnExists
from .sqlgen import CreateDatabase, DropDatabase, DatabaseRename
from .sqlgen import GrantPrivileges, DatabaseExists, DatabaseStats
# --------------------------------------------------------------------------- #
logger = logging.getLogger(__name__)
Base = declarative_base()
ConcreteBase = ConcreteBase


class ORMDatabase:
    """Connects and established a session maker to the Sqlalchemy Database."""

    def __init__(self, credentials):
        self._credentials = credentials
        self._database_uri = f'postgresql://'\
            f'{credentials.user}:{credentials.password}' \
            f'@{credentials.host}:{credentials.port}/'

        self._database = create_engine(
            f'{self._database_uri}{self._credentials.dbname}')

    def reset(self):
        Base.metadata.clear()
        Base.metadata.create_all(self._database, checkfirst=False)

    @property
    def database(self):
        return self._database

    @property
    def session(self):
        return sessionmaker(self._database)

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
            credentials.dbname))

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

    def read_query(self, name: str, command) -> pd.DataFrame:
        """Uses pandas to read sql and turn a dataframe."""
        self.bind_connection()
        df = pd.read_sql(sql=command.cmd,
                         con=self._connection,
                         params=command.params)
        return df

    def execute(self, name: str, command) -> \
        Union[int, bool, str, float, datetime, list, pd.DataFrame,
              np.array]:
        """Executes a single sql command on the object."""

        cursor = self.bind_connection()

        response = cursor.execute(command.cmd, command.params)
        command.executed = datetime.now()

        self.release_connection(cursor)

        logger.info(
            "{} was completed successfully."
            .format(command.description))

        return response

    def execute_all(self, name: str, command: list) -> None:
        """Executes a series of different SQL command within a transaction."""

        cursor = self.bind_connection()

        responses = []

        for command in command:
            response = cursor.execute(command.cmd, command.params)
            command.executed = datetime.now()
            responses.append(response)

        self.release_connection(cursor)

        cmds = "\n".join([command.description for command in command])
        logger.info(
            "The following command completed successfully:\n    {}."
            .format(cmds))

        return responses

    def execute_many(self, name: str, command) -> None:
        """Executes a single SQL command on a multiple objects."""

        cursor = self.bind_connection()

        response = cursor.executemany(command.cmd, command.params)
        command.executed = datetime.now()

        self.release_connection(cursor)

        logger.info(
            "{} was completed successfully."
            .format(command.description))

        return response

    def execute_query(self, name: str, command) -> \
        Union[int, bool, str, float, datetime, list, pd.DataFrame,
              np.array]:
        """Executes a single sql command on the object."""

        cursor = self.bind_connection()

        cursor.execute(command.cmd, command.params)
        response = cursor.fetchall()
        command.executed = datetime.now()

        self.release_connection(cursor)

        logger.info(
            "{} was completed successfully."
            .format(command.description))

        return response

    def bind_connection(self):
        """Opens connection, sets isolation level, returns the connection."""

        ConnectionPool.initialize(self._credentials)
        self._connection = ConnectionPool.get_connection()
        self._connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = self._connection.cursor()
        return cursor

    def release_connection(self, cursor) -> None:
        """Commits and returns the connection to the pool."""

        cursor.close()
        self._connection.commit()
        ConnectionPool.return_connection(self._connection)

    def close_all_connections(self) -> None:
        ConnectionPool.close_all_connections()


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

# --------------------------------------------------------------------------- #
#                           USER ADMINISTRATION                               #
# --------------------------------------------------------------------------- #


class UserAdmin(Admin):
    """Maintains users and privileges."""

    @exception_handler()
    def create(self, credentials: dict, create_db: bool = False) \
            -> None:
        query = CreateUser()
        command = query.build(credentials, create_db)
        return self._engine.execute(command.name, command)

    @exception_handler()
    def drop(self, name):
        query = DropUser()
        command = query.build(name)
        return self._engine.execute(name, command)

    @exception_handler()
    def exists(self, name) -> None:
        query = UserExists()
        command = query.build(name)
        return self._engine.execute_query(name, command)[0][0]

# --------------------------------------------------------------------------- #
#                         DATABASE ADMINISTRATION                             #
# --------------------------------------------------------------------------- #


class DBAdmin(Admin):
    """Database administration i.e. creating and dropping databases.

    Arguments
        credentials (dict): Dictionary of authorizing credentials.

    """

    temp_database = 'tempdb'

    @exception_handler()
    def create(self, name) -> None:
        query = CreateDatabase()
        command = query.build(name)
        self._engine.execute(name, command)

    @exception_handler()
    def exists(self, name) -> bool:

        query = DatabaseExists()
        command = query.build(name)
        return self._engine.execute_query(name, command)[0][0]

    @exception_handler()
    def drop(self, name) -> None:
        query = DropDatabase()
        command = query.build(name)
        self._engine.execute(name, command)

    @exception_handler()
    def rename(self, name: str, newname: str) -> None:
        query = DatabaseRename()
        command = query.build(name, newname)
        self._engine.execute(name, command)

    @exception_handler()
    def grant(self, name, user) -> None:
        query = GrantPrivileges()
        command = query.build(name, user)
        self._engine.execute(name, command)

    @exception_handler()
    def backup(self, credentials: dict, filepath: str) -> None:

        command = ['pg_dump',
                   '--dbname=postgresql://{}:{}@{}:{}/{}'.format(
                       credentials.user,
                       credentials.password,
                       credentials.host,
                       credentials.port,
                       credentials.dbname),
                   '-Fc -f',
                   filepath,
                   '-v']

        self._run_process(command)

        logger.info("Executed BACKUP on the {} database".format(
            credentials.dbname))

    @exception_handler()
    def restore(self, credentials: dict, filepath: str) -> None:

        command = ['pg_restore',
                   '--no-owner',
                   '--dbname=postgresql://{}:{}@{}:{}/{}'.format(
                       credentials.user,
                       credentials.password,
                       credentials.host,
                       credentials.port,
                       credentials.dbname),
                   '-v',
                   filepath]

        self._run_process(command)

        logger.info("Database restored from {}".format(filepath))

    @exception_handler()
    def load(self, credentials: dict, filepath: str) -> None:

        command = 'pg_restore --no-owner -d {} {}'.format(
            credentials.dbname, filepath)

        self._run_process(command)

        logger.info("Executed RESTORE on the {} database".format(
            credentials.dbname))

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

# --------------------------------------------------------------------------- #
#                         TABLE ADMINISTRATION                                #
# --------------------------------------------------------------------------- #


class TableAdmin(Admin):
    """Database table administration."""

    @exception_handler()
    def create(self, name: str, command: SQLCommand) -> None:
        """Creates a table according to the schema defined in the file.

        Arguments
            name (str): The name for the table.
            command (SQLCommand): SQL command in SQLCommand format.

        """
        self._engine.execute(name, command)

    @exception_handler()
    def drop(self, name: str) -> None:
        query = DropTable()
        command = query.build(name)
        return self._engine.execute(name, command)

    @exception_handler()
    def exists(self, name: str) -> None:
        query = TableExists()
        command = query.build(name, self._credentials.dbname)
        return self._engine.execute_query(name, command)

    @exception_handler()
    def add_columns(self, name: str, columns: list) -> None:
        """Adds one or more columns to a table.

        Arguments:
            name (str): the name of the table
            columns (dict): Dictionary with column names as keys and
                datatypes as values. Other column constraints are not
                supported to date.
        """

        query = ColumnInserter()
        command = query.build(name, columns)
        return self._engine.execute(name, command)

    @exception_handler()
    def column_exists(self, name: str, column: str) -> None:
        """Adds one or more columns to a table.

        Arguments:
            name (str): Name of the table.
            column (str): Name of the column.
        """

        query = ColumnExists()
        command = query.build(name, column)
        return self._engine.execute_query(name, command)

    @exception_handler()
    def drop_columns(self, name: str, columns: list) -> None:
        """Drops one or more columns from a table.

        Arguments:
            name (str): the name of the table
            columns (list): a list of column names to drop.
        """

        query = ColumnRemover()
        command = query.build(name, columns)
        return self._engine.execute(name, command)

# --------------------------------------------------------------------------- #
#                      DATABASE ACCESS OBJECT                                 #
# --------------------------------------------------------------------------- #


class DAO:
    """Database Access Object."""

    def __init__(self, credentials: dict) -> None:
        self._credentials = credentials
        self._connection = ConnectionPool.initialize(credentials)

    def create(self, tablename: str, columns: list, values: list) -> None:
        """Inserts a row into the designated table

        Arguments
            tablename (str): Name of the table into which insertion occurs.
            columns (list): List of string names of columns
            values (list): List of values inserted into the above.
        """
        pass

    def read(self, tablename, command) -> pd.DataFrame:
        """Reads data from the named table using the command.

        Arguments
            tablename (str): Name of the table into which insertion occurs.
            command (SQLCommand): An SQLCommand object
        """
        pass

    def update(self, tablename: str, columns: list, values: list,
               where: dict) -> None:
        """Updates a row into the designated table where key = value.

        Arguments
            tablename (str): Name of the table into which insertion occurs.
            columns (list): List of string names of columns to be updated.
            values (list): List of values to update into the above.
            where (dict): Dictionary of key, value pairs that specify a row.

        """
        pass

    def delete(self, tablename: str, where: dict) -> None:
        """Deletes a row from the designated table where key = value.

        Arguments
            tablename (str): Name of the table into which insertion occurs.
            where (dict): Dictionary of key, value pairs that specify a row.

        """
        pass

    def stats(self):
        """Returns statistics on the current database."""
        query = DatabaseStats()
        command = query.build()
        df = pd.read_sql(sql=command.cmd, con=self._connection,
                         params=command.params)
        return df
