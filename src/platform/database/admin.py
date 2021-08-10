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
# Modified : Tuesday, August 10th 2021, 4:03:47 am                            #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
"""Database setup module."""
import logging
from subprocess import Popen, PIPE
import shlex

import pandas as pd

from ...utils.logger import exception_handler
from .sequel import AdminSequel, TableSequel, UserSequel
from .connect import SAConnectionFactory
from .base import Database
# --------------------------------------------------------------------------- #
logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
#                      DATABASE ADMINISTRATION                                #
# --------------------------------------------------------------------------- #


class DBAdmin(Database):
    """Database administration class."""

    def __init__(self, credentials: dict, autocommit: bool = True):
        super(DBAdmin, self).__init__(credentials=credentials,
                                      autocommit=autocommit)
        self._sequel = AdminSequel()

    @exception_handler()
    def create(self, name: str) -> None:
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
        self._modify(sequel)

    @exception_handler()
    def exists(self, name: str) -> None:
        """Checks existance of a named database.

        Arguments
            connection (psycopg2.connection): Connection to postgres database.
            name (str): Name of database to check.

        """
        sequel = self._sequel.exists(name)
        response = self._read(sequel)[0][0]
        return response

    @exception_handler()
    def terminate_database_processes(self, name: str) -> None:
        """Terminates activity on a database.

        Arguments
            connection (psycopg2.connection): Connect to the database
            name (str): The name of the database to be dropped.

        """

        sequel = self._sequel.terminate(name)
        self._modify(sequel)

    @exception_handler()
    def delete(self, name: str) -> None:
        """Drops a database if it exists.

        Arguments
            connection (psycopg2.connection): Connect to the database
            name (str): The name of the database to be dropped.

        """

        self._connection.set_session(autocommit=True)
        sequel = self._sequel.delete(name)
        self._modify(sequel)

    @exception_handler()
    def backup(self, dbname: str, filepath: str) -> None:
        """Backs up database to the designated filepath

        Arguments
            credentials (dict): Credentials for the database
            filepath(str): Location of the backup file.

        """
        USER = self._credentials['user']
        HOST = self._credentials['host']
        PORT = self._credentials['port']
        PASSWORD = self._credentials['password']
        DBNAME = dbname

        command = ['pg_dump',
                   '--dbname=postgresql://{}:{}@{}:{}/{}'.format(USER,
                                                                 PASSWORD,
                                                                 HOST,
                                                                 PORT,
                                                                 dbname),
                   '-Fc',
                   '-f', filepath,
                   '-v']

        self._run_process(command)

        logger.info("Backed up database {} to {}".format(
            DBNAME, filepath))

    @exception_handler()
    def restore(self, dbname: str, filepath: str) -> None:

        USER = self._credentials['user']
        HOST = self._credentials['host']
        PASSWORD = self._credentials['password']
        PORT = self._credentials['port']
        DBNAME = dbname

        command = ['pg_restore',
                   '--no-owner',
                   '--dbname=postgresql://{}:{}@{}:{}/{}'.format(USER,
                                                                 PASSWORD,
                                                                 HOST,
                                                                 PORT,
                                                                 dbname),
                   '-v',
                   filepath]
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
class UserAdmin(Database):

    def __init__(self, credentials: dict, autocommit: bool = True):
        super(UserAdmin, self).__init__(credentials=credentials,
                                        autocommit=autocommit)
        self._sequel = UserSequel()

    @exception_handler()
    def create(self, name: str) -> None:
        """Creates a user for the connected database.

        Arguments:
            connection (psycopg2.connection): Connection to the database
            name (str): The username

        """
        sequel = self._sequel.create(name)
        self._modify(sequel)

    @exception_handler()
    def exists(self, name: str) -> None:
        """Creates a user for the connected database.

        Arguments:
            connection (psycopg2.connection): Connection to the database
            name (str): The username

        """
        sequel = self._sequel.exists(name)
        response = self._read(sequel)[0][0]
        return response

    @exception_handler()
    def delete(self, name: str) -> None:
        """Creates a user for the connected database.

        Arguments:
            connection (psycopg2.connection): Connection to the database
            name (str): The username

        """
        sequel = self._sequel.delete(name)
        self._modify(sequel)

    @exception_handler()
    def grant(self, name: str, dbname: str) -> None:
        """Grants user privileges to database.

        Arguments:
            connection (psycopg2.connection): Connection to the database
            user (str): The username
            dbname (str): The name of the database
        """

        sequel = self._sequel.create(name, dbname)
        self._modify(sequel)

    @exception_handler()
    def revoke(self, name: str, dbname: str) -> None:
        """Revokes user privileges to database.

        Arguments:
            connection (psycopg2.connection): Connection to the database
            name (str): The username
            dbname (str): The name of the database
        """

        sequel = self._sequel.revoke(name, dbname)
        self._modify(sequel)


# --------------------------------------------------------------------------- #
#                          TABLE ADMINISTRATION                               #
# --------------------------------------------------------------------------- #
class TableAdmin(Database):

    def __init__(self, credentials: dict, autocommit: bool = True):
        super(TableAdmin, self).__init__(credentials=credentials,
                                         autocommit=autocommit)
        self._sequel = TableSequel()
        self._sa_connection_factory = SAConnectionFactory

    @exception_handler()
    def create(self, filepath: str) -> None:
        """Creates one or more tables defined in the designated filepath.

        Arguments:
            filepath (str): The location of the file containing the DDL.

        """
        response = self._process_ddl(filepath)
        return response

    @exception_handler()
    def load(self, name: str, data: pd.DataFrame,
             schema: str = 'public', **kwargs) \
            -> None:
        """Loads a table from a pandas DataFrame object.

        Arguments:
            name(str): The name of the table.
            schema(str): The schema for the table.
            data(pd.DataFrame): DataFrame containing the data
            kwargs(dict): Arguments passed to pandas.

        Raises
            ValueError: If the table already exists and the
                if_exists parameter = 'fail'.
        """
        self._sa_connection_factory.initialize(self._credentials)
        connection = self._sa_connection_factory.get_connection()

        data.to_sql(name=name, con=connection, schema=schema, **kwargs)
        self._sa_connection_factory.return_connection(connection)

    @exception_handler()
    def exists(self, name: str,
               schema: str = 'public') -> None:
        """Checks existance of a named database.

        Arguments
            name(str): Name of database to check.
            schema (str): The namespace for the table.

        """

        sequel = self._sequel.exists(name, schema)
        response = self._read(sequel)
        return response

    @exception_handler()
    def delete(self, name: str, schema: str = 'public') \
            -> None:
        """Drops the designated table

        Arguments
            name(str): The name of the table to be dropped.
            schema (str): The namespace for the table.

        """
        sequel = self._sequel.delete(name, schema)
        self._modify(sequel)

    @exception_handler()
    def column_exists(self, name: str, column: str,
                      schema: str = 'public') -> None:
        """Checks existance of a named database.

        Arguments
            name (str): Name of table to check.
            column (str): The column to check.
            schema (str): The namespace for the table.

        """

        sequel = self._sequel.column_exists(name, schema, column)
        response = self._read(sequel)[0][0]
        return response

    @exception_handler()
    def get_columns(self, name: str,
                    schema: str = 'public') -> None:
        """Return the column names for table.

        Arguments
            name(str): Name of table
            schema (str): The namespace for the table.

        """

        sequel = self._sequel.get_columns(name, schema)
        response = self._read(sequel)
        return response
