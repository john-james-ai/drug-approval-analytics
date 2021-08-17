#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# =========================================================================== #
# Project  : Drug Approval Analytics                                          #
# Version  : 0.1.0                                                            #
# File     : \src\data\database\sequel.py                               #
# Language : Python 3.9.5                                                     #
# --------------------------------------------------------------------------  #
# Author   : John James                                                       #
# Company  : nov8.ai                                                          #
# Email    : john.james@nov8.ai                                               #
# URL      : https://github.com/john-james-sf/drug-approval-analytics         #
# --------------------------------------------------------------------------  #
# Created  : Monday, July 19th 2021, 2:26:36 pm                               #
# Modified : Tuesday, August 17th 2021, 6:48:02 am                            #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
"""SQL Generator: Generates SQL strings compatible with psycopg2.

"""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Union

from psycopg2 import sql


# --------------------------------------------------------------------------- #
#                              SQL COMMAND                                    #
# --------------------------------------------------------------------------- #


@dataclass
class Sequel:
    """Class that encapsulates a sql sequel, its name and parameters."""
    name: str
    cmd: sql.SQL
    description: str = field(default=None)
    query_context: str = field(default=None)
    object_type: str = field(default=None)
    object_name: str = field(default=None)
    params: tuple = field(default=())
# --------------------------------------------------------------------------- #
#                            ADMIN SEQUEL BASE                                #
# --------------------------------------------------------------------------- #


class AdminSequelBase(ABC):
    """Defines the API for Administrative SQL queries."""

    @abstractmethod
    def create(self, name: str, *args, **kwargs) -> Sequel:
        pass

    @abstractmethod
    def delete(self, name: str) -> Sequel:
        pass

    @abstractmethod
    def exists(self, name: str) -> Sequel:
        pass


# --------------------------------------------------------------------------- #
#                          ACCESS SEQUEL BASE                                 #
# --------------------------------------------------------------------------- #


class AccessSequelBase(ABC):
    """Defines the API for Access related SQL queries."""

    @abstractmethod
    def read(self, name: str, *args, **kwargs) -> Sequel:
        pass

    @abstractmethod
    def create(self, name: str, *args, **kwargs) -> Sequel:
        pass

    @abstractmethod
    def update(self, name: str, *args, **kwargs) -> Sequel:
        pass

    @abstractmethod
    def delete(self, name: str, *args, **kwargs) -> Sequel:
        pass


# =========================================================================== #
#                         ADMINISTRATIVE QUERIES                              #
# =========================================================================== #


# --------------------------------------------------------------------------- #
#                             DATABASE SEQUEL                                 #
# --------------------------------------------------------------------------- #
class DatabaseSequel(AdminSequelBase):

    def create(self, name: str) -> Sequel:
        sequel = Sequel(
            name="create_database",
            description="Created {} database".format(name),
            query_context='admin',
            object_type='database',
            object_name=name,
            cmd=sql.SQL("CREATE DATABASE {} ;").format(
                sql.Identifier(name))
        )

        return sequel

    def exists(self, name: str) -> Sequel:
        sequel = Sequel(
            name="database exists",
            description="Checked existence of {} database.".format(name),
            query_context='admin',
            object_type='database',
            object_name=name,
            cmd=sql.SQL("""SELECT EXISTS(
                    SELECT datname FROM pg_catalog.pg_database
                    WHERE lower(datname) = lower(%s));"""),
            params=tuple((name,))
        )

        return sequel

    def delete(self, name: str) -> Sequel:
        sequel = Sequel(
            name="drop database",
            description="Dropped {} database if it exists.".format(name),
            query_context='admin',
            object_type='database',
            object_name=name,
            cmd=sql.SQL("DROP DATABASE IF EXISTS {};").format(
                sql.Identifier(name))
        )

        return sequel

    def terminate_database(self, name: str) -> Sequel:
        sequel = Sequel(
            name="terminate_database_processes",
            description="Terminated processes on {} database if it exists."
            .format(name),
            query_context='admin',
            object_type='database',
            object_name=name,
            cmd=sql.SQL("""SELECT pg_terminate_backend(pg_stat_activity.pid)
                        FROM pg_stat_activity
                        WHERE 
                        pg_stat_activity.pid <> pg_backend_pid() AND 
                        pg_stat_activity.datname = {};""").format(
                sql.Placeholder()
            ),
            params=tuple((name,))
        )

        return sequel

    def activity(self) -> Sequel:
        sequel = Sequel(
            name="activity",
            description="Get activity from pg_stat_activity.",
            query_context='admin',
            object_type='database',
            object_name="activity",
            cmd=sql.SQL("""SELECT * FROM pg_stat_activity;"""),
            params=None
        )

        return sequel

# --------------------------------------------------------------------------- #
#                             DATABASE SCHEMA                                 #
# --------------------------------------------------------------------------- #


class SchemaSequel(AdminSequelBase):

    def create(self, name: str) -> Sequel:
        sequel = Sequel(
            name="create_schema",
            description="Created SCHEMA IF NOT EXISTS {}".format(name),
            query_context='admin',
            object_type='database',
            object_name=name,
            cmd=sql.SQL("CREATE SCHEMA {};").format(
                sql.Identifier(name))
        )

        return sequel

    def exists(self, name: str) -> Sequel:
        sequel = Sequel(
            name="database exists",
            description="Checked existence of {} database.".format(name),
            query_context='admin',
            object_type='database',
            object_name=name,
            cmd=sql.SQL("""SELECT EXISTS(
                    SELECT schema_name FROM information_schema.schemata
                    WHERE lower(schema_name) = lower(%s));"""),
            params=tuple((name,))
        )

        return sequel

    def delete(self, name: str) -> Sequel:
        sequel = Sequel(
            name="drop_schema",
            description="Dropped schema {}.".format(name),
            query_context='admin',
            object_type='database',
            object_name=name,
            cmd=sql.SQL("DROP SCHEMA {};").format(
                sql.Identifier(name))
        )

        return sequel


# --------------------------------------------------------------------------- #
#                              TABLES SEQUEL                                  #
# --------------------------------------------------------------------------- #


class TableSequel(AdminSequelBase):

    def create(self, name: str, filepath: str) -> Sequel:
        sequel = Sequel(
            name="create_table",
            description="Created table {} from SQL ddl in {}".format(
                name, filepath),
            query_context='admin',
            object_type='table',
            object_name=name,
            cmd=None,
            params=filepath
        )
        return sequel

    def batch_create(self, filepath: str) -> Sequel:
        sequel = Sequel(
            name="batch_create_tables",
            description="Create tables from SQL ddl in {}".format(filepath),
            query_context='admin',
            object_type='table',
            object_name="batch",
            cmd=None,
            params=filepath
        )
        return sequel

    def exists(self, name: str, schema: str) -> Sequel:
        sequel = Sequel(
            name="table_exists",
            description="Checked existence of table {}".format(name),
            query_context='admin',
            object_type='table',
            object_name=name,
            cmd=sql.SQL("""SELECT 1 FROM information_schema.tables
                                WHERE table_schema = {}
                                AND table_name = {}""").format(
                sql.Placeholder(),
                sql.Placeholder()
            ),
            params=(schema, name,)
        )

        return sequel

    def delete(self, name: str, schema: str) -> Sequel:
        sequel = Sequel(
            name="delete_table",
            description="Drop table {}.{}".format(schema, name),
            query_context='admin',
            object_type='table',
            object_name=name,
            cmd=sql.SQL("DROP TABLE IF EXISTS {}.{};").format(
                sql.Identifier(schema),
                sql.Identifier(name)
            )
        )
        return sequel

    def batch_delete(self, filepath) -> Sequel:
        sequel = Sequel(
            name="delete_tables",
            description="Drop tables from SQL ddl in {}".format(filepath),
            query_context='admin',
            object_type='table',
            object_name="batch",
            cmd=None,
            params=filepath
        )
        return sequel

    def column_exists(self, name: str, schema: str, column: str) -> Sequel:

        sequel = Sequel(
            name="column_exists",
            description="Checked existence of column {} in {} table".format(
                column, name),
            query_context='admin',
            object_type='table',
            object_name=name,
            cmd=sql.SQL("""SELECT 1 FROM information_schema.columns
                                WHERE table_schema = {}
                                AND table_name = {}
                                AND column_name = {}""").format(
                sql.Placeholder(),
                sql.Placeholder(),
                sql.Placeholder()
            ),
            params=(schema, name, column,)
        )

        return sequel

    def get_columns(self, name: str, schema: str) -> Sequel:

        sequel = Sequel(
            name="column_exists",
            description="Obtained columns for {}.{} table".format(
                schema, name),
            query_context='admin',
            object_type='table',
            object_name=name,
            cmd=sql.SQL("""SELECT column_name FROM information_schema.columns
                                WHERE table_schema = {}
                                AND table_name = {}""").format(
                sql.Placeholder(),
                sql.Placeholder()
            ),
            params=(schema, name)
        )

        return sequel

    def create_column(self, name: str, schema: str, column: str,
                      datatype: str) -> Sequel:

        sequel = Sequel(
            name="column_exists",
            description="Add column {} to {}.{} table".format(
                column, schema, name),
            query_context='admin',
            object_type='table',
            object_name=name,
            cmd=sql.SQL("""ALTER TABLE {}.{} ADD {} {};""").format(
                sql.Identifier(schema),
                sql.Identifier(name),
                sql.Identifier(column),
                sql.Placeholder()
            ),
            params=(datatype,)
        )

        return sequel

    def tables(self, schema: str = 'public') -> Sequel:
        sequel = Sequel(
            name="tables",
            description="Selected table names in {} schema.".format(schema),
            query_context='admin',
            object_type='database',
            object_name=schema,
            cmd=sql.SQL("""SELECT table_name FROM information_schema.tables
       WHERE table_schema = {}""").format(
                sql.Placeholder()
            ),
            params=(schema,)
        )
        return sequel

# --------------------------------------------------------------------------- #
#                               USER SEQUEL                                   #
# --------------------------------------------------------------------------- #


class UserSequel(AdminSequelBase):

    def create(self, name: str, password: str) -> Sequel:
        sequel = Sequel(
            name="create_user",
            description="Created user {}".format(name),
            query_context='admin',
            object_type='user',
            object_name=name,
            cmd=sql.SQL("CREATE USER {} WITH PASSWORD {} CREATEDB;").format(
                sql.Identifier(name),
                sql.Placeholder()
            ),
            params=(password,)
        )

        return sequel

    def delete(self, name: str) -> Sequel:
        sequel = Sequel(
            name="drop_user",
            description="Dropped user {}".format(name),
            query_context='admin',
            object_type='user',
            object_name=name,
            cmd=sql.SQL("DROP USER IF EXISTS {};").format(
                sql.Identifier(name))
        )

        return sequel

    def exists(self, name: str) -> Sequel:
        sequel = Sequel(
            name="user_exists",
            description="Checked existence of user {}".format(name),
            query_context='admin',
            object_type='user',
            object_name=name,
            cmd=sql.SQL("SELECT 1 FROM pg_roles WHERE rolname ={};").format(
                sql.Placeholder()),
            params=tuple((name,))
        )

        return sequel

    def grant(self, name: str, dbname: str) -> Sequel:
        sequel = Sequel(
            name="grant",
            description="Granted privileges on database {} to {}"
            .format(dbname, name),
            query_context='admin',
            object_type='user',
            object_name=name,
            cmd=sql.SQL("GRANT ALL PRIVILEGES ON DATABASE {} TO {} ;")
            .format(
                sql.Identifier(dbname),
                sql.Identifier(name))
        )

        return sequel

    def revoke(self, name: str, dbname: str) -> Sequel:
        sequel = Sequel(
            name="revoke",
            description="Revoked privileges on database, names, and sequences {} from {}"
            .format(dbname, name),
            query_context='admin',
            object_type='user',
            object_name=name,
            cmd=sql.SQL("""REVOKE ALL PRIVILEGES ON DATABASE {} FROM {} ;
            REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM {};
            REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM {};
            REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public FROM {};""")
            .format(
                sql.Identifier(dbname),
                sql.Identifier(name),
                sql.Identifier(name),
                sql.Identifier(name),
                sql.Identifier(name)
            )
        )

        return sequel


# =========================================================================== #
#                               ACCESS QUERIES                                #
# =========================================================================== #
class AccessSequel(AccessSequelBase):

    def _get(self, name: str, schema: str, columns: list = None,
             filter_key: str = None,
             filter_value: Union[str, int, float] = None)\
            -> Sequel:

        sequel = Sequel(
            name="select",
            description="Selected {} from {}.{} where {} = {}".format(
                columns, schema, name, filter_key, filter_value
            ),
            query_context='access',
            object_type='table',
            object_name=name,
            cmd=sql.SQL("SELECT {} FROM {}.{} WHERE {} = {};").format(
                sql.SQL(", ").join(map(sql.Identifier, columns)),
                sql.Identifier(schema),
                sql.Identifier(name),
                sql.Identifier(filter_key),
                sql.Placeholder()),
            params=(filter_value,)
        )
        return sequel

    def _get_all_columns_all_rows(self, name: str, schema: str,
                                  columns: list = None,
                                  filter_key: str = None,
                                  filter_value: Union[str, int, float] = None)\
            -> Sequel:

        sequel = Sequel(
            name="select",
            description="Selected * from {}.{}".format(
                schema, name
            ),
            query_context='access',
            object_type='table',
            object_name=name,
            cmd=sql.SQL("SELECT * FROM {}.{};").format(
                sql.Identifier(schema),
                sql.Identifier(name)
            )
        )
        return sequel

    def _get_all_rows(self, name: str, schema: str, columns: list = None,
                      filter_key: str = None,
                      filter_value: Union[str, int, float] = None) -> Sequel:

        sequel = Sequel(
            name="select",
            description="Selected {} from {}.{}".format(
                columns, schema, name
            ),
            query_context='access',
            object_type='table',
            object_name=name,
            cmd=sql.SQL("SELECT {} FROM {}.{};").format(
                sql.SQL(", ").join(map(sql.Identifier, columns)),
                sql.Identifier(schema),
                sql.Identifier(name)
            )
        )
        return sequel

    def _get_all_columns(self, name: str, schema: str,
                         columns: list = None,
                         filter_key: str = None,
                         filter_value: Union[str, int, float] = None)\
            -> Sequel:

        sequel = Sequel(
            name="select",
            description="Selected * from {}.{} where {} = {}".format(
                schema, name, filter_key, filter_value
            ),
            query_context='access',
            object_type='table',
            object_name=name,
            cmd=sql.SQL("SELECT * FROM {}.{} WHERE {} = {};").format(
                sql.Identifier(schema),
                sql.Identifier(name),
                sql.Identifier(filter_key),
                sql.Placeholder()
            ),
            params=(filter_value,)
        )
        return sequel

    def read(self, name: str, schema: str, columns: list = None,
             filter_key: str = None,
             filter_value: Union[str, int, float] = None) -> Sequel:

        if (filter_key is None and filter_value is None) != \
                (filter_key is None or filter_value is None):
            raise ValueError("where values not completely specified.")

        if (columns is not None and filter_key is not None):
            # Returns selected columns from selected rows
            return self._get(name=name, schema=schema, columns=columns,
                             filter_key=filter_key, filter_value=filter_value)
        elif (columns is not None):
            # Returns all rows, selected columns
            return self._get_all_rows(name=name, schema=schema,
                                      columns=columns,
                                      filter_key=filter_key,
                                      filter_value=filter_value)

        elif (filter_key is not None):
            # Returns all columns, selected rows
            return self._get_all_columns(name=name, schema=schema,
                                         columns=columns,
                                         filter_key=filter_key,
                                         filter_value=filter_value)

        else:
            return self._get_all_columns_all_rows(name=name, schema=schema,
                                                  columns=columns,
                                                  filter_key=filter_key,
                                                  filter_value=filter_value)

    def create(self, name: str, schema: str, columns: list,
               values: list) -> Sequel:

        if (len(columns) != len(values)):
            raise ValueError(
                "Number of columns doesn't match number of values")

        sequel = Sequel(
            name="insert",
            description="Inserted into {}.{} {} values {}".format(
                schema, name, columns, name
            ),
            query_context='access',
            object_type='table',
            object_name=name,
            cmd=sql.SQL("INSERT into {}.{} ({}) values ({});")
            .format(
                sql.Identifier(schema),
                sql.Identifier(name),
                sql.SQL(', ').join(map(sql.Identifier, tuple((*columns,)))),
                sql.SQL(', ').join(sql.Placeholder() * len(columns))
            ),
            params=(*values,)
        )

        return sequel

    def update(self, name: str, schema: str, column: str,
               value: Union[str, float, int], filter_key: str,
               filter_value: Union[str, float, int]) -> Sequel:

        sequel = Sequel(
            name="update",
            description="Updated {}.{} setting {} = {} where {} = {}".format(
                schema, name, column, value, filter_key, filter_value
            ),
            query_context='access',
            object_type='table',
            object_name=name,
            cmd=sql.SQL("UPDATE {}.{} SET {} = {} WHERE {} = {}").format(
                sql.Identifier(schema),
                sql.Identifier(name),
                sql.Identifier(column),
                sql.Placeholder(),
                sql.Identifier(filter_key),
                sql.Placeholder()
            ),
            params=(value, filter_value,)
        )

        return sequel

    def delete(self, name: str, schema: str, filter_key: str,
               filter_value: Union[str, float, int]) -> Sequel:

        sequel = Sequel(
            name="delete",
            description="Deleted from {}.{} where {} = {}".format(
                schema, name, filter_key, filter_value
            ),
            query_context='access',
            object_type='table',
            object_name=name,
            cmd=sql.SQL("DELETE FROM {} WHERE {} = {}").format(
                sql.Identifier(name),
                sql.Identifier(filter_key),
                sql.Placeholder()
            ),
            params=(filter_value,)
        )

        return sequel

    def begin(self) -> Sequel:

        sequel = Sequel(
            name="begin",
            description="Started transaction.",
            query_context='access',
            object_type='transaction',
            object_name='connection',
            cmd=sql.SQL("START TRANSACTION;")
        )

        return sequel
