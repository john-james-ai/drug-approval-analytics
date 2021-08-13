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
# Modified : Thursday, August 12th 2021, 9:39:29 pm                           #
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
from ..config import DBCredentials
# --------------------------------------------------------------------------- #
logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
#                      DATABASE ADMINISTRATION                                #
# --------------------------------------------------------------------------- #


class DBAdmin(Database):
    """Database administration class."""

    def __init__():
        """Database Administration class

        CRUD operations for databases and tables.

        Dependencies:
            AdminSequel (Sequel): Class serving Sequel objects containing
                parameterized SQL statements.

        """
        self._sequel = AdminSequel()

    @exception_handler()
    def create_database(self, name: str, connection) -> None:
        """Creates a database

        Arguments
            name (str): The name of the database.
            connection (Psycopg2 Database Connection)

        Raises:
            ERROR: user 'username' is not allowed to create/drop databases
            ERROR: createdb: database "name" already exists
            ERROR: database path may not contain single quotes
            ERROR: CREATE DATABASE: may not be called in a transaction block
            ERROR: Unable to create database directory 'path'.

        """

        sequel = self._sequel.create_database(name)
        response = self._execute(connection, sequel)
        return response

    @exception_handler()
    def database_exists(self, name: str, connection) -> None:
        """Checks existance of a named database.

        Arguments
            name (str): Name of database to check.
            connection (Psycopg2 Database Connection)

        """
        sequel = self._sequel.database_exists(name)
        response = self._execute(sequel, connection)
        if response.fetchall:
            return response.fetchall[0][0]
        else:
            return False

    @exception_handler()
    def terminate_database_processes(self, name: str, connection) -> None:
        """Terminates activity on a database.

        Arguments
            name (str): The name of the database to be dropped.
            connection (Psycopg2 Database Connection)

        """

        sequel = self._sequel.terminate_database(name)
        response = self._execute(connection, sequel)
        return response

    @exception_handler()
    def delete_database(self, name: str, connection) -> None:
        """Drops a database if it exists.

        Arguments
            name (str): The name of the database to be dropped.
            connection (Psycopg2 Database Connection)

        """

        sequel = self._sequel.delete_database(name)
        response = self._execute(sequel, connection)
        return response

    @exception_handler()
    def backup_database(self, dbname: str, filepath: str) -> None:
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
                   '--dbname=Psycopg2ql://{}:{}@{}:{}/{}'.format(USER,
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
    def restore_database(self, dbname: str, filepath: str) -> None:

        USER = self._credentials['user']
        HOST = self._credentials['host']
        PASSWORD = self._credentials['password']
        PORT = self._credentials['port']
        DBNAME = dbname

        command = ['pg_restore',
                   '--no-owner',
                   '--dbname=Psycopg2ql://{}:{}@{}:{}/{}'.format(USER,
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

    @exception_handler()
    def create_tables(self, filepath: str, connection) -> None:
        """Creates one or more tables defined in the designated filepath.

        Arguments:
            filepath (str): The location of the file containing the DDL.
            connection (Psycopg2 Database Connection)

        """
        sequel = self._sequel.create_tables(filepath)
        self._execute_ddl(sequel, connection)

    @exception_handler()
    def load_table(self, name: str, data: pd.DataFrame, connection,
                   schema: str = 'public', **kwargs) \
            -> None:
        """Loads a table from a pandas DataFrame object.

        Arguments:
            name(str): The name of the table.
            schema(str): The schema for the table.
            data(pd.DataFrame): DataFrame containing the data
            connection(SAConnection): SQLAlchemy connection.
            kwargs(dict): Arguments passed to pandas.

        Raises
            ValueError: If the table already exists and the
                if_exists parameter = 'fail'.
        """

        data.to_sql(name=name, con=connection, schema=schema,
                    if_exists='append', **kwargs)

    @exception_handler()
    def table_exists(self, name: str, connection,
                     schema: str = 'public') -> None:
        """Checks existance of a named database.

        Arguments
            name(str): Name of database to check.
            connection (Psycopg2 Database Connection)
            schema (str): The namespace for the table.

        """

        sequel = self._sequel.table_exists(name, schema)
        response = self._execute(sequel, connection)

        if response.fetchall:
            return response.fetchall[0][0]
        else:
            return False

    @exception_handler()
    def delete_tables(self, filepath: str, connection) -> None:
        """Deletes one or more tables by ddl defined in the designated filepath.

        Arguments:
            filepath (str): The location of the file containing the DDL.
            connection (Psycopg2 Database Connection)

        """
        sequel = self._sequel.delete_tables(filepath)
        self._execute_ddl(sequel, connection)

    @exception_handler()
    def column_exists(self, name: str, column: str, connection,
                      schema: str = 'public') -> None:
        """Checks existance of a named database.

        Arguments
            name (str): Name of table to check.
            column (str): The column to check.
            connection (Psycopg2 Database Connection)
            schema (str): The namespace for the table.

        """

        sequel = self._sequel.column_exists(name, schema, column)
        response = self._execute(sequel, connection)
        if response.fetchall:
            return response.fetchall[0][0]
        else:
            return False

    @exception_handler()
    @property
    def tables(self, name: str, connection, schema: str = 'public') -> None:
        """Checks existance of a named database.

        Arguments
            name (str): Name of database to check.
            connection (Psycopg2 Database Connection)
            schema (str): Schema for table. Defaults to public

        """
        sequel = self._sequel.tables(schema)
        response = self._execute(sequel, connection)
        tablelist = []
        for table in response.fetchall:
            tablelist.append(table)
        return tablelist

    @property
    @exception_handler()
    def columns(self, name: str, connection,
                schema: str = 'public') -> None:
        """Return the column names for table.

        Arguments
            name(str): Name of table
            connection (Psycopg2 Database Connection)
            schema (str): The namespace for the table.

        """

        sequel = self._sequel.get_columns(name, schema)
        response = self._execute(sequel, connection)

        return response.fetchall

# --------------------------------------------------------------------------- #
#                           USER ADMINISTRATION                               #
# --------------------------------------------------------------------------- #


class UserAdmin(Database):

    def __init__(self):
        self._sequel = UserSequel()

    @exception_handler()
    def create(self, name: str, password: str, connection) -> None:
        """Creates a user for the connected database.

        Arguments:
            connection (psycopg2.connection): Connection to the database
            name (str): The username

        """
        sequel = self._sequel.create_user(name, password)
        self._execute(sequel, connection)

    @exception_handler()
    def exists(self, name: str, connection) -> None:
        """Creates a user for the connected database.

        Arguments:
            connection (psycopg2.connection): Connection to the database
            name (str): The username

        """
        sequel = self._sequel.user_exists(name)
        response = self._execute(sequel, connection)
        if response.fetchall:
            return response.fetchall[0][0]
        else:
            return False

    @exception_handler()
    def delete(self, name: str, connection) -> None:
        """Creates a user for the connected database.

        Arguments:
            connection (psycopg2.connection): Connection to the database
            name (str): The username

        """
        sequel = self._sequel.delete_user(name)
        self._execute(sequel, connection)

    @exception_handler()
    def grant(self, name: str, dbname: str, connection) -> None:
        """Grants user privileges to database.

        Arguments:
            connection (psycopg2.connection): Connection to the database
            user (str): The username
            dbname (str): The name of the database
        """

        sequel = self._sequel.grant_user(name, dbname)
        self._execute(sequel, connection)

    @exception_handler()
    def revoke(self, name: str, dbname: str, connection) -> None:
        """Revokes user privileges to database.

        Arguments:
            connection (psycopg2.connection): Connection to the database
            name (str): The username
            dbname (str): The name of the database
        """

        sequel = self._sequel.revoke_user(name, dbname)
        self._execute(sequel, connection)
