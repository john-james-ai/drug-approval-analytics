#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# =========================================================================== #
# Project  : Drug Approval Analytics                                          #
# Version  : 0.1.0                                                            #
# File     : \src\infrastructure\database\connect.py                          #
# Language : Python 3.9.5                                                     #
# --------------------------------------------------------------------------  #
# Author   : John James                                                       #
# Company  : nov8.ai                                                          #
# Email    : john.james@nov8.ai                                               #
# URL      : https://github.com/john-james-sf/drug-approval-analytics         #
# --------------------------------------------------------------------------  #
# Created  : Tuesday, August 3rd 2021, 4:47:23 am                             #
# Modified : Tuesday, August 17th 2021, 7:03:38 am                            #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
"""Core internal Base, Connection, and ConnectionPool classes."""
from abc import ABC, abstractmethod
import logging

import psycopg2
from psycopg2 import pool


from .sequel import DatabaseSequel, TableSequel, UserSequel, SchemaSequel
from .connect import PGConnectionPool, SAConnectionPool, Connection
from .config import DBCredentials
from ...utils.logger import exception_handler
from ...utils.files import string_replace
# --------------------------------------------------------------------------- #
logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
#                        DATABASE CONFIGURATION                               #
# --------------------------------------------------------------------------- #
class DatabaseConfiguration:
    """Defines the configuration of the DatabaseBuilder

    Arguments:
        name (str): The name of the database
        schema (str): The name of the table schema
        replace_if_exists (bool): If true, delete database if it exists.
        dba_credentials (DBCredentials): Postgres database credentials
        owner_pg_credentials (DBCredentials): Owner credentials for postgres
        owner_db_credentials (DBCredentials): Owner credentials for new database
        create_table_ddl_filepath (str): Path to create table ddl.
        drop_table_ddl_filepath (str): Path to drop table ddl.
        table_data (dict): Dictionary of tablename (key), data (value)
            pairs to be loaded as part of the database initialization


    """

    def __init__(self, name: str,
                 schema: str,
                 dba_credentials: DBCredentials,
                 owner_pg_credentials: DBCredentials,
                 owner_db_credentials: DBCredentials,
                 create_table_ddl_filepath: str,
                 drop_table_ddl_filepath: str,
                 table_data: dict,
                 replace_if_exists: bool = False) -> None:

        self._name = name
        self._schema - schema
        self._dba_credentials = dba_credentials
        self._owner_pg_credentials = owner_pg_credentials
        self._owner_db_credentials = owner_db_credentials
        self._create_table_ddl_filepath = create_table_ddl_filepath
        self._drop_table_ddl_filepath = drop_table_ddl_filepath
        self._table_data = table_data
        self._replace_if_exists = replace_if_exists

    @property
    def name(self) -> str:
        return self._name

    @property
    def dba_credentials(self) -> DBCredentials:
        return self._owner_pg_credentials

    @property
    def owner_pg_credentials(self) -> DBCredentials:
        return self._dba_credentials

    @property
    def schema_table_ddl(self) -> str:
        return self._schema_table_ddl

    @property
    def table_data(self) -> dict:
        return self._table_data

# --------------------------------------------------------------------------- #
#                          DATABASE BUILDER                                   #
# --------------------------------------------------------------------------- #


class DatabaseBuilder(ABC):
    """Base class for constructing database objects.

    Arguments:
        configuration (DatabaseConfiguration): Object containing the name,
            connection dba_credentials, the location of table creation ddl, and
            data to be loaded at initialization.
    """

    def __init__(self, configuration: DatabaseConfiguration) -> None:
        self._configuration = configuration
        self._database = None

    @abstractmethod
    def reset(self) -> None:
        pass

    @abstractmethod
    def build_owner(self) -> None:
        pass

    @abstractmethod
    def build_database(self) -> None:
        pass

    @abstractmethod
    def build_schema(self) -> None:
        pass

    @abstractmethod
    def build_ddl(self) -> None:
        pass

    @abstractmethod
    def build_tables(self) -> None:
        pass

    @abstractmethod
    def initialize(self) -> None:
        pass

    @property
    @abstractmethod
    def database(self) -> None:
        pass


# --------------------------------------------------------------------------- #
#                         METADATABASE BUILDER                                #
# --------------------------------------------------------------------------- #
class MetaDatabaseBuilder(DatabaseBuilder):
    """Metadata database builder

    The superuser having dba_credentials creates the owner role and grants
    privilges on the postgres database. Next, the owner creates the
    database, then creates another set of credentials for the new database.
    This 3rd user will build and load the tables. All users have two
    sets of credentials: one for postgres database, and one for the
    new database created.

    Arguments:
        configuration (DatabaseConfiguration): Object containing the name,
            connection dba_credentials, the location of table creation ddl, and
            data to be loaded at initialization.
    """

    def __init__(self, configuration: DatabaseConfiguration) -> None:
        super(MetaDatabaseBuilder, self).__init__(configuration)

    @exception_handler()
    def reset(self) -> None:
        self._database = Database()

    @exception_handler()
    def build_owner(self) -> None:
        connection = Connection(self._configuration.dba_credentials)
        connection.open_connection()

        # Create user and grant access to postgres database
        if not self._database.user_exists(
                self._configuration.owner_pg_credentials.name, connection):
            self._database.create_user(self._configuration.owner_pg_credentials.name,
                                       self._configuration.owner_pg_credentials.password,
                                       connection)
            self._database.grant(self._configuration.owner_pg_credentials.name,
                                 self._name, connection)

        # Create user credentials for new database
        config = DBCredentials()
        config.create(name=self._configuration.owner_db_credentials.name,
                      user=self._configuration.owner_db_credentials.user,
                      password=self._configuration.owner_db_credentials.password,
                      host=self._configuration.owner_db_credentials.host,
                      dbname=self._configuration.owner_db_credentials.dbname,
                      port=self._configuration.owner_pg_credentials.port
                      )

        connection.commit()
        connection.close_connection()

    @exception_handler()
    def build_database(self) -> None:
        connection = Connection(self._configuration.owner_db_credentials)
        connection.open_connection()
        if self._database.exists(self._name, connection):
            if self._replace_if_exists:
                self._database.delete(self._name, connection)
                self._database.create(self._name, connection)
            else:
                pass
        else:
            self._database.create(self._name, connection)
        connection.commit()
        connection.close_connection()

    @exception_handler()
    def build_schema(self):
        connection = Connection(self._configuration.owner_db_credentials)
        connection.open_connection()
        self._database.create_schema(self._schema, connection)

        connection.commit()
        connection.close_connection()

    @exception_handler()
    def build_ddl(self):
        string_replace(self._configuration.create_table_ddl_filepath,
                       'public', configuration.schema)

    @exception_handler()
    def build_tables(self) -> None:
        # First need to hackerously change the schema in the ddl from 'public'
        # to the designated schema.
        connection = Connection(configuration.dba_credentials)
        connection.begin_transaction()
        # If replace if exists, drop the tables if they exist.
        if self._configuration._replace_if_exists:
            self._database.delete(
                self._configuration._drop_table_ddl_filepath, connection)
        self._database.create_tables(
            self._configuration.create_table_ddl_filepath, connection)

        connection.end_transaction()
        connection.close_connection()

    @exception_handler()
    def initialize(self) -> None:
        connection = Connection(configuration.dba_credentials)
        connection.begin_transaction()
        schema = self._configuration.schema
        if_exists = 'fail'

        if self._configuration._replace_if_exists:
            if_exists = 'replace'

        for table, data in configuration.table_data.items():
            data.to_sql(name=table, con=connection, schema=schema,
                        if_exists=if_exists, **kwargs)

        connection.end_transaction()
        connection.close_connection()

    @property
    def database(self) -> None:
        return self._database

# --------------------------------------------------------------------------- #
#                               DATABASE                                      #
# --------------------------------------------------------------------------- #
# ----------------------------------------------------------------------- #
#                             DATABASE                                    #
# ----------------------------------------------------------------------- #


class Response:
    """Class representing responses from database commands."""

    def __init__(self, cursor=None, execute=None, fetchone=None,
                 fetchall=None, description=None, rowcount: int = 0):
        self.cursor = cursor
        self.execute = execute
        self.fetchall = fetchall
        self.fetchone = fetchone
        self.rowcount = rowcount
        self.description = description


# --------------------------------------------------------------------------- #
class Database(ABC):

    def __init__(self):
        self._database_sequel = DatabaseSequel()
        self._table_sequel = TableSequel()
        self._user_sequel = UserSequel()
        self._schema_sequel = SchemaSequel

    # ----------------------------------------------------------------------- #
    #                             DATABASE                                    #
    # ----------------------------------------------------------------------- #
    @exception_handler()
    def create(self, name: str, connection: Connection) -> Response:
        """Creates a database

        Arguments:
            name (str): The name of the database to create.
            connection (Connection): Connection to postgres database

        Returns:
            Response object containing the results from database command.
        """
        # Can't create database in a transaction block. Save current value
        # for autocommit and set the property to True for the create
        # process, then revert autocommit property to original value.
        autocommit = connection.autocommit
        connection.autocommit = True

        sequel = self._database_sequel.create(name)
        response = self.execute(sequel, connection)

        connection.autcommit = autocommit

        return response

    @exception_handler()
    def exists(self, name: str, connection: Connection) -> Response:
        """Evaluates whether the named database exists.

        Arguments:
            name (str): The name of the database to find.
            connection (Connection): Connection to postgres database

        Returns:
            Response object containing the results from database command.
        """

        sequel = self._database_sequel.create(name)
        response = self.execute(sequel, connection)
        connection.return_connection()
        return response

    @exception_handler()
    def delete(self, name: str, connection: Connection) -> Response:
        """Deletes a database

        Arguments:
            name (str): The name of the database to delete.
            connection (Connection): Connection to postgres database

        Returns:
            Response object containing the results from database command.
        """
        # Can't delete database in a transaction block. Save current value
        # for autocommit and set the property to True for the delete
        # process, then revert autocommit property to original value.
        autocommit = connection.autocommit
        connection.autocommit = True

        sequel = self._database_sequel.delete(name)
        response = self.execute(sequel, connection)

        connection.autcommit = autocommit

        return response

    @exception_handler()
    def terminate_database_processes(self, name: str,
                                     connection: Connection) -> None:
        """Terminates activity on a database.

        Arguments
            name (str): The name of the database to be dropped.
            connection (Psycopg2 Database Connection)

        """

        sequel = self._database_sequel.terminate_database(name)
        response = self.execute(sequel, connection)
        return response

    @exception_handler()
    def activity(self, connection: Connection) -> None:
        """Get activity on a database.

        Arguments
            connection (Psycopg2 Database Connection)

        """

        sequel = self._database_sequel.activity()
        response = self.execute(sequel, connection)
        return response.fetchall

    @exception_handler()
    def backup(self, dbname: str, filepath: str) -> None:
        """Backs up database to the designated filepath

        Arguments
            dba_credentials (dict): Credentials for the database
            filepath(str): Location of the backup file.

        """
        USER = self._dba_credentials['user']
        HOST = self._dba_credentials['host']
        PORT = self._dba_credentials['port']
        PASSWORD = self._dba_credentials['password']
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
    def restore(self, dbname: str, filepath: str) -> None:

        USER = self._dba_credentials['user']
        HOST = self._dba_credentials['host']
        PASSWORD = self._dba_credentials['password']
        PORT = self._dba_credentials['port']
        DBNAME = dbname

        command = ['pg_restore',
                   '--no-owner_pg_credentials',
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

    # ----------------------------------------------------------------------- #
    #                               SCHEMA                                    #
    # ----------------------------------------------------------------------- #
    @exception_handler()
    def create_schema(self, name: str,
                      connection: Connection) -> None:
        """Creates a schema in the currently connected database.

        Arguments:
            name (str): The name of the schema
            connection (Connection): Connection to the database

        """
        sequel = self._schema_sequel.create(name)
        self.execute_ddl(sequel, connection)

    @exception_handler()
    def schema_exists(self, name: str,
                      connection: Connection) -> None:
        """Checks existance of a named schema.

        Arguments
            name(str): Name of schema to check.
            connection (Connection)

        """

        sequel = self._schema_sequel.exists(name)
        response = self.execute(sequel, connection)

        if response.fetchall:
            return response.fetchall[0][0]
        else:
            return False

    @exception_handler()
    def delete_schema(self, name: str,
                      connection: Connection) -> None:
        """Drops a schema

        Arguments:
            name(str): Name of the schema to drop
            connection (Connection)

        """
        sequel = self._schema_sequel.delete(name)
        self.execute(sequel, connection)

    # ----------------------------------------------------------------------- #
    #                               TABLES                                    #
    # ----------------------------------------------------------------------- #

    @exception_handler()
    def create_table(self, filepath: str,
                     connection: Connection) -> None:
        """Creates a single table from ddl defined in the designated filepath.

        Arguments:
            filepath (str): The location of the file containing the DDL.
            connection (Connection): Connection to the database

        """
        sequel = self._table_sequel.batch_create(filepath)
        self.execute_ddl(sequel, connection)

    @exception_handler()
    def create_tables(self, filepath: str,
                      connection: Connection) -> None:
        """Creates multiple tables from ddl defined in the designated filepath.

        Arguments:
            filepath (str): The location of the file containing the DDL.
            connection (Connection): Connection to the database

        """
        sequel = self._table_sequel.batch_create(filepath)
        self.execute_ddl(sequel, connection)

    @exception_handler()
    def table_exists(self, name: str,
                     connection: Connection,
                     schema: str = 'public') -> None:
        """Checks existance of a named table.

        Arguments
            name(str): Name of table to check.
            connection (Psycopg2 Database Connection)
            schema (str): The namespace for the table.

        """

        sequel = self._table_sequel.exists(name, schema)
        response = self.execute(sequel, connection)

        if response.fetchall:
            return response.fetchall[0][0]
        else:
            return False

    @exception_handler()
    def delete_table(self, name: str,
                     connection: Connection,
                     schema: str = 'public') -> None:
        """Drops a table

        Arguments:
            name(str): Name of table to check.
            connection (Psycopg2 Database Connection)
            schema (str): The namespace for the table.

        """
        sequel = self._table_sequel.delete(name, schema)
        self.execute(sequel, connection)

    @exception_handler()
    def delete_tables(self, filepath: str,
                      connection: Connection) -> None:
        """Batch deletes multiple tables by ddl defined in the designated filepath.

        Arguments:
            filepath (str): The location of the file containing the DDL.
            connection (Psycopg2 Database Connection)

        """
        sequel = self._table_sequel.batch_delete(filepath)
        self.execute_ddl(sequel, connection)

    @exception_handler()
    def column_exists(self, name: str, column: str,
                      connection: PGConnectionPool,
                      schema: str = 'public') -> None:
        """Checks existance of a named database.

        Arguments
            name (str): Name of table to check.
            column (str): The column to check.
            connection (Psycopg2 Database Connection)
            schema (str): The namespace for the table.

        """

        sequel = self._table_sequel.column_exists(name, schema, column)
        response = self.execute(sequel, connection)
        if response.fetchall:
            return response.fetchall[0][0]
        else:
            return False

    @exception_handler()
    @property
    def tables(self, name: str,
               connection: Connection,
               schema: str = 'public') -> None:
        """Returns a list of tables in the database

        Arguments
            name (str): Name of database to check.
            connection (Psycopg2 Database Connection)
            schema (str): Schema for table. Defaults to public

        """
        sequel = self._table_sequel.tables(schema)
        response = self.execute(sequel, connection)
        tablelist = []
        for table in response.fetchall:
            tablelist.append(table)
        return tablelist

    @exception_handler()
    def get_columns(self, name: str,
                    connection: Connection,
                    schema: str = 'public') -> None:
        """Return the column names for table.

        Arguments
            name(str): Name of table
            connection (Psycopg2 Database Connection)
            schema (str): The namespace for the table.

        """

        sequel = self._table_sequel.get_columns(name, schema)
        response = self.execute(sequel, connection)

        return response.fetchall

    # ----------------------------------------------------------------------- #
    #                                USER                                     #
    # ----------------------------------------------------------------------- #

    @exception_handler()
    def create_user(self, name: str, password: str,
                    connection: Connection) -> None:
        """Creates a user for the connected database.

        Arguments:
            connection (Connection): Connection to the database
            name (str): The username

        """
        sequel = self._user_sequel.create(name, password)
        self.execute(sequel, connection)

    @exception_handler()
    def user_exists(self, name: str,
                    connection: Connection) -> None:
        """Evaluates existence of user on database

        Arguments:
            connection (Connection): Connection to the database
            name (str): The username

        """
        sequel = self._user_sequel.exists(name)
        response = self.execute(sequel, connection)
        if response.fetchall:
            return response.fetchall[0][0]
        else:
            return False

    @exception_handler()
    def delete_user(self, name: str,
                    connection: Connection) -> None:
        """Drops user.

        Arguments:
            connection (Connection): Connection to the database
            name (str): The username

        """
        sequel = self._user_sequel.delete(name)
        self.execute(sequel, connection)

    @exception_handler()
    def grant(self, name: str, dbname: str,
              connection: Connection) -> None:
        """Grants user privileges to database.

        Arguments:
            connection (Connection): Connection to the database
            user (str): The username
            dbname (str): The name of the database
        """

        sequel = self._user_sequel.grant(name, dbname)
        self.execute(sequel, connection)

    @exception_handler()
    def revoke(self, name: str, dbname: str,
               connection: Connection) -> None:
        """Revokes user privileges to database.

        Arguments:
            connection (Connection): Connection to the database
            name (str): The username
            dbname (str): The name of the database
        """

        sequel = self._user_sequel.revoke(name, dbname)
        self.execute(sequel, connection)

    # ----------------------------------------------------------------------- #
    #                              EXECUTE                                    #
    # ----------------------------------------------------------------------- #

    @exception_handler()
    def execute_next(self, cursor) -> Response:
        response_fetchone = cursor.fetchone()
        response_description = cursor.description
        response = Response(cursor=cursor,
                            fetchone=response_fetchone,
                            description=response_description)

        return response

    @exception_handler()
    def execute_one(self, sequel: Sequel, connection: Connection) -> Response:
        cursor = connection.cursor()
        response_execute = cursor.execute(sequel.cmd, sequel.params)
        response_description = cursor.description
        response_rowcount = cursor.rowcount
        response_fetchone = cursor.fetchone()

        response = Response(cursor=cursor,
                            execute=response_execute,
                            fetchone=response_fetchone,
                            description=response_description,
                            rowcount=response_rowcount)

        logger.info(sequel.description)
        return response

    @exception_handler()
    def execute(self, sequel: Sequel, connection: Connection) -> Response:
        cursor = connection.cursor()
        response_execute = cursor.execute(sequel.cmd, sequel.params)
        response_description = cursor.description
        response_rowcount = cursor.rowcount

        try:
            response_fetchall = cursor.fetchall()
        except psycopg2.ProgrammingError:
            response_fetchall = None

        response = Response(cursor=cursor,
                            execute=response_execute,
                            fetchall=response_fetchall,
                            description=response_description,
                            rowcount=response_rowcount)
        cursor.close()
        logger.info(sequel.description)
        return response

    @exception_handler()
    def execute_ddl(self, sequel: Sequel, connection: Connection) -> None:
        """Processes SQL DDL commands from file."""

        with connection.cursor() as cursor:
            cursor.execute(open(sequel.params, "r").read())
        cursor.close()
        logger.info(sequel.description)
