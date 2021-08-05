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
# Modified : Thursday, August 5th 2021, 3:49:53 am                            #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
"""Database setup module."""
from abc import ABC, abstractmethod
import logging
from subprocess import Popen, PIPE
import shlex

import pandas as pd
from pandas import io

from ...utils.logger import exception_handler
from .sequel import DatabaseSequel, TableSequel, UserSequel
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
    def drop(self, connection, name: str) \
            -> None:
        pass

# --------------------------------------------------------------------------- #
#                      DATABASE ADMINISTRATION                                #
# --------------------------------------------------------------------------- #


class DBAdmin(Admin):
    """Database administration class."""

    def __init__(self):
        self._sequel = DatabaseSequel()

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

        sequel = self._sequel.create(name)

        connection.set_session(autocommit=True)
        cursor = connection.cursor()
        cursor.execute(sequel.cmd, sequel.params)
        cursor.close()

        logger.info(sequel.description)

    @exception_handler()
    def exists(self, connection, name: str) -> None:
        """Checks existance of a named database.

        Arguments
            connection (psycopg2.connection): Connection to postgres database.
            name (str): Name of database to check.

        """
        sequel = self._sequel.exists(name)

        cursor = connection.cursor()
        cursor.execute(sequel.cmd, sequel.params)
        response = cursor.fetchone()[0]
        cursor.close()

        logger.info(sequel.description)

        return response

    @exception_handler()
    def terminate_database_processes(self, connection, name: str) -> None:
        """Terminates activity on a database.

        Arguments
            connection (psycopg2.connection): Connect to the database
            name (str): The name of the database to be dropped.

        """

        sequel = self._sequel.terminate(name)
        cursor = connection.cursor()
        cursor.execute(sequel.cmd, sequel.params)
        cursor.close()

        logger.info(sequel.description)

    @exception_handler()
    def drop(self, connection, name: str) -> None:
        """Drops a database if it exists.

        Arguments
            connection (psycopg2.connection): Connect to the database
            name (str): The name of the database to be dropped.

        """

        connection.set_session(autocommit=True)
        sequel = self._sequel.drop(name)
        cursor = connection.cursor()
        cursor.execute(sequel.cmd, sequel.params)
        cursor.close()

        logger.info(sequel.description)

    @exception_handler()
    def backup(self, credentials: dict, dbname: str, filepath: str) -> None:
        """Backs up database to the designated filepath

        Arguments
            credentials (dict): Credentials for the database
            filepath(str): Location of the backup file.

        """
        USER = credentials['user']
        HOST = credentials['host']
        PORT = credentials['port']
        DBNAME = dbname

        command =\
            'pg_dump -h {0} -d {1} -U {2} -p {3} -Fc -f {4}'.format(
                HOST, DBNAME, USER, PORT, filepath)

        self._run_process(command)

        logger.info("Backed up database {} to {}".format(
            DBNAME, filepath))

    @exception_handler()
    def restore(self, credentials: dict, dbname: str, filepath: str) -> None:

        USER = credentials['user']
        HOST = credentials['host']
        DBNAME = dbname

        command =\
            'pg_dump -h {0} -d {1} -U {2} {3}'.format(
                HOST, DBNAME, USER, filepath)
        self._run_process(command)

        logger.info("Restored database {} from {}".format(
            DBNAME, filepath))

    @exception_handler()
    def _run_process(self, command):
        """Wrapper for Python subprocess commands."""

        if isinstance(command, str):
            command = shlex.split(command)

        proc = Popen(command,  shell=True, stdin=PIPE,
                     stdout=PIPE, stderr=PIPE, encoding='utf8')

        out, err = proc.communicate()

        if int(proc.returncode) != 0:
            if err.strip() == "":
                err = out
            mesg = "Error [%d]: %s" % (proc.returncode, command)
            mesg += "\nDetail: %s" % err
            raise Exception(mesg)

        return proc.returncode, out, err


# --------------------------------------------------------------------------- #
#                           USER ADMINISTRATION                               #
# --------------------------------------------------------------------------- #
class UserAdmin(Admin):

    def __init__(self):
        self._sequel = UserSequel()

    @exception_handler()
    def create(self, connection, name: str) -> None:
        """Creates a user for the connected database.

        Arguments:
            connection (psycopg2.connection): Connection to the database
            name (str): The username

        """
        sequel = self._sequel.create(name)
        cursor = connection.cursor()
        cursor.execute(sequel.cmd, sequel.params)
        cursor.close()

        logger.info(sequel.description)

    @exception_handler()
    def exists(self, connection, name: str) -> None:
        """Creates a user for the connected database.

        Arguments:
            connection (psycopg2.connection): Connection to the database
            name (str): The username

        """
        sequel = self._sequel.exists(name)
        cursor = connection.cursor()
        cursor.execute(sequel.cmd, sequel.params)
        response = cursor.fetchone()[0]
        cursor.close()

        logger.info(sequel.description)
        return response

    @exception_handler()
    def drop(self, connection, name: str) -> None:
        """Creates a user for the connected database.

        Arguments:
            connection (psycopg2.connection): Connection to the database
            name (str): The username

        """
        sequel = self._sequel.drop(name)
        cursor = connection.cursor()
        cursor.execute(sequel.cmd, sequel.params)
        cursor.close()

        logger.info(sequel.description)

    @exception_handler()
    def grant(self, connection, name: str, dbname: str) -> None:
        """Grants user privileges to database.

        Arguments:
            connection (psycopg2.connection): Connection to the database
            user (str): The username
            dbname (str): The name of the database
        """

        sequel = self._sequel.create(name, dbname)
        cursor = connection.cursor()
        cursor.execute(sequel.cmd, sequel.params)
        cursor.close()

        logger.info(sequel.description)

    @exception_handler()
    def revoke(self, connection, name: str, dbname: str) -> None:
        """Revokes user privileges to database.

        Arguments:
            connection (psycopg2.connection): Connection to the database
            name (str): The username
            dbname (str): The name of the database
        """

        sequel = self._sequel.revoke(name, dbname)
        cursor = connection.cursor()
        cursor.execute(sequel.cmd, sequel.params)
        cursor.close()

        logger.info(sequel.description)


# --------------------------------------------------------------------------- #
#                          TABLE ADMINISTRATION                               #
# --------------------------------------------------------------------------- #
class TableAdmin(Admin):

    def __init__(self):
        self._sequel = TableSequel()

    # ----------------------------------------------------------------------- #
    #                      TABLE ADMINISTRATION                               #
    # ----------------------------------------------------------------------- #

    @exception_handler()
    def create(self, connection, name: str, data: pd.DataFrame,
               schema: str = 'public', **kwargs) \
            -> None:
        """Creates a table from a pandas DataFrame object.

        Arguments:
            connection(sqlalchemy.engine.Connection): Database connection
            name(str): The name of the table.
            schema(str): The schema for the table.
            data(pd.DataFrame): DataFrame containing the data
            kwargs(dict): Arguments passed to pandas.

        Raises
            ValueError: If the table already exists and the
                if_exists parameter = 'fail'.
        """
        data.to_sql(name=name, con=connection, schema=schema, **kwargs)

    @exception_handler()
    def exists(self, connection, name: str,
               schema: str = 'public') -> None:
        """Checks existance of a named database.

        Arguments
            connection(psycopg2.connection): Connection to postgres database.
            name(str): Name of database to check.
            schema (str): The namespace for the table.

        """

        sequel = self._sequel.exists(name, schema)
        cursor = connection.cursor()
        cursor.execute(sequel.cmd, sequel.params)
        response = cursor.fetchone()
        cursor.close()

        logger.info(sequel.description)
        return response

    @exception_handler()
    def drop(self, connection, name: str, schema: str = 'public') \
            -> None:
        """Drops the designated table

        Arguments
            connection(sqlalchemy.engine.Connection): Database connection
            name(str): The name of the table to be dropped.
            schema (str): The namespace for the table.

        """
        sequel = self._sequel.drop(name, schema)
        io.sql.execute(sequel.cmd, connection)

        logger.info(sequel.description)

    @exception_handler()
    def add_column(self, connection, name: str, column: str, datatype: str,
                   constraint: str = None, schema: str = 'public') -> None:
        """Adds a column to a table.

        Arguments:
            connection(psycopg2.connection): Connection to postgres database.
            name(str): The name of the table.
            column (str): Name of column to add
            schema (str): The namespace for the table.


        """
        sequel = self._sequel.add_column(name, schema, column,
                                         datatype, constraint)
        cursor = connection.cursor()
        cursor.execute(sequel.cmd, sequel.params)
        cursor.close()

        logger.info(sequel.description)

    @exception_handler()
    def drop_column(self, connection, name: str, column: str,
                    schema: str = 'public') -> None:
        """Drops a table column.

        Arguments:
            connection(psycopg2.connection): Connection to postgres database.
            name(str): The name of the table.
            column (str): Name of column to drop
            schema (str): The namespace for the table.

        """
        sequel = self._sequel.drop_column(name, schema, column)
        cursor = connection.cursor()
        cursor.execute(sequel.cmd, sequel.params)
        cursor.close()

        logger.info(sequel.description)

    @exception_handler()
    def column_exists(self, connection, name: str, column: str,
                      schema: str = 'public') -> None:
        """Checks existance of a named database.

        Arguments
            connection(psycopg2.connection): Connection to postgres database.
            name(str): Name of database to check.
            schema (str): The namespace for the table.

        """

        sequel = self._sequel.column_exists(name, schema, column)
        cursor = connection.cursor()
        cursor.execute(sequel.cmd, sequel.params)
        response = cursor.fetchone()
        cursor.close()

        logger.info(sequel.description)
        return response
