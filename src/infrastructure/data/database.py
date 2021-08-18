#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# =========================================================================== #
# Project  : Drug Approval Analytics                                          #
# Version  : 0.1.0                                                            #
# File     : \src\infrastructure\data\database.py                             #
# Language : Python 3.9.5                                                     #
# --------------------------------------------------------------------------  #
# Author   : John James                                                       #
# Company  : nov8.ai                                                          #
# Email    : john.james@nov8.ai                                               #
# URL      : https://github.com/john-james-sf/drug-approval-analytics         #
# --------------------------------------------------------------------------  #
# Created  : Monday, August 16th 2021, 9:50:45 am                             #
# Modified : Wednesday, August 18th 2021, 10:45:07 am                         #
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
from .sequel import Sequel
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
        dba_pg_credentials (DBCredentials): Postgres database credentials
        dba_db_credentials (DBCredentials): Postgres credentials on new database        
        user_db_credentials (DBCredentials): User credentials for new database
        create_table_ddl_filepath (str): Path to create table ddl.
        drop_table_ddl_filepath (str): Path to drop table ddl.
        table_data (dict): Dictionary of tablename (key), data (value)
            pairs to be loaded as part of the database initialization
        replace_if_exists (bool): If true, delete database if it exists.
            If False and any of the database entities exist, an
            exception will be raised.

        Raises:
            [Entity] already exists: If replace_if_exists is False and
            any entity owner, database, or tables, already exist.


    """

    def __init__(self, name: str,
                 schema: str,
                 dba_pg_credentials: DBCredentials,
                 dba_db_credentials: DBCredentials,
                 user_db_credentials: DBCredentials,
                 create_table_ddl_filepath: str,
                 drop_table_ddl_filepath: str,
                 table_data: dict,
                 replace_if_exists: bool = True) -> None:

        self._name = name
        self._schema = schema
        self._dba_pg_credentials = dba_pg_credentials
        self._dba_db_credentials = dba_db_credentials
        self._user_db_credentials = user_db_credentials
        self._create_table_ddl_filepath = create_table_ddl_filepath
        self._drop_table_ddl_filepath = drop_table_ddl_filepath
        self._table_data = table_data
        self._replace_if_exists = replace_if_exists
        self._validate()

    def _validate(self):

        if self.dba_db_credentials.dbname != self.user_db_credentials.dbname \
            or self.dba_db_credentials.dbname != self._name \
                or self.user_db_credentials.dbname != self._name:
            msg = """BuilderConfigError: DBA dbname {} and
            owner dbname {} must match and both must equal {}.""".format(
                self.dba_db_credentials.dbname, self.user_db_credentials.dbname,
                self._name
            )
            logger.error(msg)
            raise ValueError(msg)

    @property
    def name(self) -> str:
        return self._name

    @property
    def schema(self) -> str:
        return self._schema

    @property
    def dba_pg_credentials(self) -> DBCredentials:
        return self._dba_pg_credentials

    @property
    def dba_db_credentials(self) -> DBCredentials:
        return self._dba_db_credentials

    @property
    def user_db_credentials(self) -> DBCredentials:
        return self._user_db_credentials

    @property
    def create_table_ddl_filepath(self) -> str:
        return self._create_table_ddl_filepath

    @property
    def drop_table_ddl_filepath(self) -> str:
        return self._drop_table_ddl_filepath

    @property
    def table_data(self) -> dict:
        return self._table_data

    @property
    def replace_if_exists(self) -> str:
        return self._replace_if_exists

# --------------------------------------------------------------------------- #
#                          DATABASE BUILDER                                   #
# --------------------------------------------------------------------------- #


class DatabaseBuilder(ABC):
    """Base class for constructing database objects.

    Arguments:
        configuration (DatabaseConfiguration): Object containing the name,
            connection dba_pg_credentials, the location of table creation ddl, and
            data to be loaded at initialization.
    """

    def __init__(self, configuration: DatabaseConfiguration) -> None:
        self._builder_config = configuration
        self._database = None

    @abstractmethod
    def reset(self) -> None:
        pass

    @abstractmethod
    def build_user(self) -> None:
        pass

    @abstractmethod
    def build_database(self) -> None:
        pass

    @abstractmethod
    def build_schema(self) -> None:
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

    @property
    def config(self) -> DatabaseConfiguration:
        return self._builder_config

# --------------------------------------------------------------------------- #
#                         METADATABASE BUILDER                                #
# --------------------------------------------------------------------------- #


class MetaDatabaseBuilder(DatabaseBuilder):
    """Metadata database builder

    The superuser having dba_pg_credentials creates the owner role and grants
    privilges on the postgres database. Next, the owner creates the
    database, then creates another set of credentials for the new database.
    This 3rd user will build and load the tables. All users have two
    sets of credentials: one for postgres database, and one for the
    new database created.

    Arguments:
        configuration (DatabaseConfiguration): Object containing the name,
            connection dba_pg_credentials, the location of table creation ddl, and
            data to be loaded at initialization.
    """

    def __init__(self, configuration: DatabaseConfiguration) -> None:
        super(MetaDatabaseBuilder, self).__init__(configuration)

    @exception_handler()
    def _rollback_user(self, user: str, dbname: str, connection: Connection) -> None:
        # Confirm user exists
        if not self._database.user_exists(user, connection):
            msg = "\n\nRollback Warning: User {} does not exist. No action taken".format(
                user)
            logger.warn(msg)
            return

        # Remove user. This revokes privileges on the database and any dependent
        # databases, then deletes the user.
        self._database.remove_user(user, dbname, connection)
        msg = "\n\nRollback: User {} removed from {}".format(
            user, connection.dbname)
        logger.info(msg)

    @exception_handler()
    def _rollback_tables(self, connection: Connection) -> None:
        self._database.delete_tables(
            self._builder_config.drop_table_ddl_filepath, connection)
        msg = "\n\nRollback: Tables removed from {}".format(
            connection.dbname)
        logger.info(msg)

    @exception_handler()
    def _rollback_database(self, connection: Connection) -> None:
        # Confirm the database to delete isn't the current database.
        if self._builder_config.name == connection.dbname:
            msg = "\n\nRollback Warning: Active database {} cannot be dropped. No action taken.".format(
                connection.dbname)
            logger.warn(msg)
            return

        self._database.delete(self._builder_config.name, connection)
        msg = "\n\nRollback: Database {} dropped.".format(
            self._builder_config.name)
        logger.info(msg)

    @exception_handler()
    def _update_ddl(self) -> None:

        if not self._builder_config.replace_if_exists:
            # If not replacing, create tables only if they don't already exist
            string_replace(self._builder_config.create_table_ddl_filepath,
                           'CREATE TABLE public', 'CREATE TABLE IF NOT EXISTS public')

        # Add IF EXISTS option to DROP TABLE
        string_replace(self._builder_config.drop_table_ddl_filepath,
                       'DROP TABLE public', "DROP TABLE IF EXISTS public")

        # Update CREATE table DDL with requested schema.
        string_replace(self._builder_config.create_table_ddl_filepath,
                       'public', self._builder_config.schema)

        # Update DROP table DDL with requested schema.
        string_replace(self._builder_config.drop_table_ddl_filepath,
                       'public', self._builder_config.schema)

    # --------------------------------------------------------------------------- #
    @exception_handler()
    def reset(self) -> None:
        # Instantiate a database object
        self._database = Database()

        # Add schema and IF EXISTS type options to DDL
        self._update_ddl()

        # Grab a connection in autocommit mode. Database changes can't be made
        # in transaction mode.
        connection = Connection(self._builder_config.dba_db_credentials,
                                autocommit=True, postgres=True)
        with connection:
            # If the database already exists
            if self._database.exists(self._builder_config.name, connection):

                # If replace_if_exists is True
                if self._builder_config.replace_if_exists:

                    # Rollback tables
                    self._rollback_tables(connection)

                    self._rollback_user(
                        self._builder_config.user_db_credentials.user,
                        self._builder_config.name,
                        connection)

                else:
                    msg = "DatabaseError: {} already exists.".format(
                        self._builder_config.name)
                    logger.error(msg)
                    raise DatabaseError(msg)

    @exception_handler()
    def build_database(self) -> None:

        connection = Connection(
            self._builder_config.dba_pg_credentials, autocommit=True, postgres=True)

        # Rollback database
        self._rollback_database(connection)
        # Create the database
        self._database.create(self._builder_config.name, connection)

    @exception_handler()
    def build_schema(self) -> None:
        # User creates new transaction on new database.
        connection = Connection(self._builder_config.dba_db_credentials,
                                autocommit=True, postgres=True)

        with connection:
            # User creates the schema
            self._database.create_schema(name=self._builder_config.schema,
                                         connection=connection)

    @exception_handler()
    def build_tables(self) -> None:
        connection = Connection(self._builder_config.dba_db_credentials,
                                autocommit=True, postgres=True)

        with connection:
            self._database.create_tables(
                self._builder_config.create_table_ddl_filepath, connection)

    @exception_handler()
    def initialize(self) -> None:
        # Initialize database with data source information

        schema = self._builder_config.schema
        if_exists = 'fail'

        if self._builder_config._replace_if_exists:
            if_exists = 'append'

        connection = Connection(self._builder_config.dba_db_credentials,
                                autocommit=True, postgres=False)

        for table, data in self._builder_config.table_data.items():
            data.to_sql(name=table, con=connection.cursor, schema=schema,
                        if_exists=if_exists)

    @exception_handler()
    def build_user(self) -> None:
        # Create a new transaction on the new database
        connection = Connection(self._builder_config.dba_db_credentials,
                                autocommit=True, postgres=True)
        with connection:

            # Create the user on the new database
            self._database.create_user(self._builder_config.user_db_credentials.user,
                                       self._builder_config.user_db_credentials.password,
                                       connection)

            self._database.grant(self._builder_config.user_db_credentials.user,
                                 self._builder_config.user_db_credentials.dbname,
                                 connection)

        # Viola

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
        self._schema_sequel = SchemaSequel()

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
        # Can't create database in a transaction block. First approach was
        # to save the current autocommit value and reset it to False for
        # the create command and then setting back afterwards. Second
        # transaction management is not create's job.
        sequel = self._database_sequel.create(name)
        response = self.execute(sequel, connection)
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

        sequel = self._database_sequel.exists(name)
        response = self.execute(sequel, connection)
        return response.fetchall[0][0]

    @exception_handler()
    def delete(self, name: str, connection: Connection) -> Response:
        """Deletes a database

        Arguments:
            name (str): The name of the database to delete.
            connection (Connection): Connection to postgres database

        Returns:
            Response object containing the results from database command.
        """
        sequel = self._database_sequel.delete(name)
        response = self.execute(sequel, connection)
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
            dba_pg_credentials (dict): Credentials for the database
            filepath(str): Location of the backup file.

        """
        USER = self._dba_pg_credentials['user']
        HOST = self._dba_pg_credentials['host']
        PORT = self._dba_pg_credentials['port']
        PASSWORD = self._dba_pg_credentials['password']
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

        USER = self._dba_pg_credentials['user']
        HOST = self._dba_pg_credentials['host']
        PASSWORD = self._dba_pg_credentials['password']
        PORT = self._dba_pg_credentials['port']
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
        self.execute(sequel, connection)

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
            name (str): The username
            dbname (str): The name of the database
            connection (Connection): Connection to the database
        """

        sequel = self._user_sequel.grant(name, dbname)
        self.execute(sequel, connection)

    @exception_handler()
    def revoke(self, name: str, dbname: str,
               connection: Connection) -> None:
        """Revokes user privileges to database.

        Arguments:
            name (str): The username
            dbname (str): The name of the database
            connection (Connection): Connection to the database
        """

        sequel = self._user_sequel.revoke(name, dbname)
        self.execute(sequel, connection)

    @exception_handler()
    def remove_user(self, name: str, dbname: str, connection: Connection) -> None:
        """Revokes all privileges and drops user.

        Arguments:
            name (str): The username
            connection (Connection): Connection to the database


        """
        if self.user_exists(name, connection):
            self.revoke(name, dbname, connection)
            self.delete_user(name, connection)

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
