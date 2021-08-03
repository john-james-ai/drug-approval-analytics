#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# =========================================================================== #
# Project  : Drug Approval Analytics                                          #
# Version  : 0.1.0                                                            #
# File     : \src\platform\data\admin.py                                      #
# Language : Python 3.9.5                                                     #
# --------------------------------------------------------------------------  #
# Author   : John James                                                       #
# Company  : nov8.ai                                                          #
# Email    : john.james@nov8.ai                                               #
# URL      : https://github.com/john-james-sf/drug-approval-analytics         #
# --------------------------------------------------------------------------  #
# Created  : Tuesday, August 3rd 2021, 12:27:05 pm                            #
# Modified : Tuesday, August 3rd 2021, 7:21:13 pm                             #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
"""Database setup module."""
from abc import ABC, abstractmethod
import logging
from subprocess import Popen, PIPE
import tempfile

import pandas as pd
import psycopg2
from psycopg2 import sql

from ...utils.logger import exception_handler
# --------------------------------------------------------------------------- #
logger = logging.getLogger(__name__)


class Admin(ABC):
    """Abstract base class for database administration classes."""

    @abstractmethod
    def create(self, connection, *args, **kwargs) \
            -> None:
        pass

    @abstractmethod
    def exists(self, connection, name: str) -> None:
        pass

    @abstractmethod
    def copy(self, connection, source: str,
             target: str, *args, **kwargs) -> None:
        pass

    @abstractmethod
    def drop(self, connection, name: str) \
            -> None:
        pass

# --------------------------------------------------------------------------- #
#                      DATABASE ADMINISTRATION                                #
# --------------------------------------------------------------------------- #


class DBAdmin(Admin):
    """Database administration class."""

    @exception_handler()
    def create(self, connection, name: str) -> None:
        """Creates a database

        Arguments
            connection (psycopg2.connection): Connection to postgres database.
            name (str): The name of the database.

        Raises:
            ERROR: user 'username' is not allowed to create/drop databases
            ERROR: createdb: database "name" already exists
            ERROR: database path may not contain single quotes
            ERROR: CREATE DATABASE: may not be called in a transaction block
            ERROR: Unable to create database directory 'path'.

        """

        command = sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name))

        cursor = connection.cursor()
        cursor.execute(command)
        cursor.close()

        logger.info("Created {} database".format(name))

    @exception_handler()
    def exists(self, connection, name: str) -> None:
        """Checks existance of a named database.

        Arguments
            connection (psycopg2.connection): Connection to postgres database.
            name (str): Name of database to check.

        """

        try:
            command = sql.SQL("SELECT VERSION()")
            cursor = connection.cursor()
            cursor.execute(command)

        except psycopg2.OperationalError:
            return False

        finally:
            cursor.close()
        return True

    @exception_handler()
    def copy(self, connection, source: str, target: str) -> None:
        """Copies a source database to a target database on same server.

        Arguments
            connection (psycopg2.connection): Connect to the database
            source (str): The name of the source database.
            target (str): The name of the target database.
            *args, **kwargs: Arguments passed to pandas

        Raises:
            ERROR: user 'username' is not allowed to create/drop databases
            ERROR: createdb: database "name" already exists
            ERROR: database path may not contain single quotes
            ERROR: CREATE DATABASE: may not be called in a transaction block
            ERROR: Unable to create database directory 'path'.
            ERROR: Could not initialize database directory.

        """

        command = sql.SQL("CREATE DATABASE {} WITH TEMPLATE {};").format(
            sql.Identifier(source),
            sql.Identifier(target)
        )

        cursor = connection.cursor()
        cursor.execute(command)
        cursor.close()

        logger.info("Copied database {} to {}".format(
            source, target))

    @exception_handler()
    def drop(self, connection, name: str) -> None:
        """Drops a database if it exists.

        Arguments
            connection (psycopg2.connection): Connect to the database
            name (str): The name of the database to be dropped.

        """

        command = sql.SQL("DROP DATABASE IF EXISTS {};").format(
            sql.Identifier(name)
        )

        cursor = connection.cursor()
        cursor.execute(command)
        cursor.close()

        logger.info("Dropped database {}.".format(name))

    @exception_handler()
    def backup(self, credentials: dict, filepath: str) -> None:
        """Backs up database to the designated filepath

        Arguments
            credentials (dict): Credentials for the database
            filepath(str): Location of the backup file.

        """
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

        logger.info("Backed up database {} to {}".format(
            credentials['dbname'], filepath))

    @exception_handler()
    def restore(self, credentials: dict, filepath: str) -> None:

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

        logger.info("Restored database {} from {}".format(
            credentials['dbname'], filepath))

    @exception_handler()
    def _run_process(self, command):
        """Wrapper for Python subprocess commands."""

        process = Popen(command,  shell=True, stdin=PIPE,
                        stdout=PIPE, stderr=PIPE, encoding='utf8')

        response = process.communicate()[0]

        if int(process.returncode) != 0:
            raise Exception("Return code: {}\nResponse: {}"
                            .format(process.returncode, response))


# --------------------------------------------------------------------------- #
#                          TABLE ADMINISTRATION                               #
# --------------------------------------------------------------------------- #


class TableAdmin(Admin):
    """Table administration class. """

    @exception_handler()
    def create(self, connection, data: pd.DataFrame,
               name: str, **kwargs) \
            -> None:
        """Creates a table from a pandas DataFrame object.

        Arguments:
            connection (sqlalchemy.engine.Connection): Database connection
            data (pd.DataFrame): DataFrame containing the data
            name (str): The name of the table.
            kwargs (dict): Arguments passed to pandas.

        Raises
            ValueError: If the table already exists and the
                if_exists parameter = 'fail'.
        """
        data.to_sql(name=name, con=connection, **kwargs)

    @exception_handler()
    def exists(self, connection, name: str) -> None:
        """Checks existance of a named database.

        Arguments
            connection (psycopg2.connection): Connection to postgres database.
            name (str): Name of database to check.

        """

        command = sql.SQL("""SELECT FROM information_schema.tables
            WHERE table_name = {};""")
        cursor = connection.cursor()
        response = cursor.execute("SELECT EXISTS ({});".format(command))
        return response[0]

    @exception_handler()
    def copy(self, connection, source: str,
             target: str) -> None:
        """Copies source table to target table in the same database.

        Arguments:
            connection (psycopg2.connection): Connection to postgres database.
            source (str): Source table name.
            target (str): Target table name.
        """
        # Create tempfile
        fp = tempfile.NamedTemporaryFile(mode="w+b", suffix=".csv")

        # Copy source to temp file, and
        command = sql.SQL("COPY {} TO {} WITH DELIMITER ',';").format(
            sql.Identifier(source),
            sql.Identifier(fp.name)
        )
        cursor = connection.cursor()
        cursor.execute(command)
        cursor.close()

        # Copy tempfile to target
        command = sql.SQL("COPY {} FROM {} WITH DELIMITER ',';").format(
            sql.Identifier(fp.name),
            sql.Identifier(target)
        )
        cursor = connection.cursor()
        cursor.execute(command)
        cursor.close()

        # Close the tempfile
        fp.close()

    @exception_handler()
    def drop(self, connection, name: str) \
            -> None:
        """Drops the designated table

        Arguments
            connection (psycopg2.connection): Connection to postgres database.
            name (str): The name of the table to be dropped.

        """
        command = sql.SQL("DROP TABLE IF EXISTS {}").format(
            sql.Identifier(name)
        )
        cursor = connection.cursor()
        cursor.execute(command)
        cursor.close()

    @exception_handler()
    def add_columns(self, connection, name: str, columns: list)\
            -> None:
        """Adds a column or columns to a table.

        Arguments:
            connection (psycopg2.connection): Connection to postgres database.
            name (str): The name of the table.
            columns (list): List of dictionaries. Each dictionary
                has column name for key and data type for value.

        """
        cursor = connection.cursor()
        for column in columns:
            command = sql.SQL("""ALTER TABLE {} ADD COLUMN
                              IF NOT EXISTS {} {}
                              """).format(
                sql.Identifier(name),
                sql.Identifier(column.key()),
                sql.Identifier(column.value())
            )
            cursor.execute(command)
        cursor.close()

    @exception_handler()
    def drop_columns(self, connection, name: str, columns: list) \
            -> None:
        """Drops a table column.

        Arguments:
            connection (psycopg2.connection): Connection to postgres database.
            name (str): The name of the table.
            columns (list): List of one or more columns to drop.

        """
        cursor = connection.cursor()
        for column in columns:
            command = sql.SQL("""ALTER TABLE {} DROP COLUMN
                              IF EXISTS {}
                              """).format(
                sql.Identifier(name),
                sql.Identifier(column)
            )
            cursor.execute(command)
        cursor.close()
