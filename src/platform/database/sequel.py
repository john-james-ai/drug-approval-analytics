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
# Modified : Tuesday, August 10th 2021, 3:41:29 am                            #
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
    def drop(self, name: str) -> Sequel:
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
    def get(self, name: str, *args, **kwargs) -> Sequel:
        pass

    @abstractmethod
    def add(self, name: str, *args, **kwargs) -> Sequel:
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
class AdminSequel(AdminSequelBase):

    def create(self, name: str) -> Sequel:
        sequel = Sequel(
            name="create_database",
            description="Created {} database".format(name),
            cmd=sql.SQL("CREATE DATABASE {} ;").format(
                sql.Identifier(name))
        )

        return sequel

    def drop(self, name: str) -> Sequel:
        sequel = Sequel(
            name="drop database",
            description="Dropped {} database if it exists.".format(name),
            cmd=sql.SQL("DROP DATABASE IF EXISTS {} ;").format(
                sql.Identifier(name))
        )

        return sequel

    def terminate(self, name: str) -> Sequel:
        sequel = Sequel(
            name="terminate_database_processes",
            description="Terminated processes on {} database if it exists."
            .format(name),
            cmd=sql.SQL("""SELECT pg_terminate_backend(pg_stat_activity.pid)
                        FROM pg_stat_activity
                        WHERE pg_stat_activity.datname = {};""").format(
                sql.Placeholder()
            ),
            params=tuple((name,))
        )

        return sequel

    def exists(self, name: str) -> Sequel:
        sequel = Sequel(
            name="database exists",
            description="Checked existence of {} database.".format(name),
            cmd=sql.SQL("""SELECT EXISTS(
                    SELECT datname FROM pg_catalog.pg_database
                    WHERE lower(datname) = lower(%s));"""),
            params=tuple((name,))
        )

        return sequel


# --------------------------------------------------------------------------- #
#                               USER SEQUEL                                   #
# --------------------------------------------------------------------------- #
class UserSequel(AdminSequelBase):

    def create(self, name: str) -> Sequel:
        sequel = Sequel(
            name="create_user",
            description="Created user {}".format(name),
            cmd=sql.SQL("CREATE USER {} CREATEDB;").format(
                sql.Identifier(name))
        )

        return sequel

    def drop(self, name: str) -> Sequel:
        sequel = Sequel(
            name="drop_user",
            description="Dropped user {}".format(name),
            cmd=sql.SQL("DROP USER IF EXISTS {};").format(
                sql.Identifier(name))
        )

        return sequel

    def exists(self, name: str) -> Sequel:
        sequel = Sequel(
            name="user_exists",
            description="Checked existence of user {}".format(name),
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
            cmd=sql.SQL("GRANT ALL PRIVILEGES ON DATABASE {} TO {} ;")
            .format(
                sql.Identifier(dbname),
                sql.Identifier(name))
        )

        return sequel

    def revoke(self, name: str, dbname: str) -> Sequel:
        sequel = Sequel(
            name="revoke",
            description="Revoked privileges on database {} to {}"
            .format(dbname, name),
            cmd=sql.SQL("REVOKE ALL PRIVILEGES ON DATABASE {} TO {} ;")
            .format(
                sql.Identifier(dbname),
                sql.Identifier(name))
        )

        return sequel
# --------------------------------------------------------------------------- #
#                              TABLES SEQUEL                                  #
# --------------------------------------------------------------------------- #


class TableSequel(AdminSequelBase):

    def create(self, name: str, schema: str = 'public') -> Sequel:
        msg = "Tables are created with data using pandas to_sql facility. "
        msg += "No query required."
        raise NotImplementedError(msg)

    def drop(self, name: str, schema: str = 'public') -> Sequel:
        # names = [schema, name]
        # name = ".".join(names)
        sequel = Sequel(
            name="drop_table",
            description="Dropped table {}.{}".format(schema, name),
            cmd=sql.SQL("DROP TABLE IF EXISTS {}.{} CASCADE;").format(
                sql.Identifier(schema),
                sql.Identifier(name))
        )

        return sequel

    def exists(self, name: str, schema: str) -> Sequel:
        sequel = Sequel(
            name="table_exists",
            description="Checked existence of table {}".format(name),
            cmd=sql.SQL("""SELECT 1 FROM information_schema.tables
                                WHERE table_schema = {}
                                AND table_name = {}""").format(
                sql.Placeholder(),
                sql.Placeholder()
            ),
            params=(schema, name,)
        )

        return sequel

    def column_exists(self, name: str, schema: str, column: str) -> Sequel:

        sequel = Sequel(
            name="column_exists",
            description="Checked existence of column {} in {} table".format(
                column, name),
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
            cmd=sql.SQL("""SELECT column_name FROM information_schema.columns
                                WHERE table_schema = {}
                                AND table_name = {}""").format(
                sql.Placeholder(),
                sql.Placeholder()
            ),
            params=(schema, name)
        )

        return sequel

    def add_column(self, name: str, schema: str, column: str,
                   datatype: str) -> Sequel:

        sequel = Sequel(
            name="column_exists",
            description="Add column {} to {}.{} table".format(
                column, schema, name),
            cmd=sql.SQL("""ALTER TABLE {}.{} ADD {} {};""").format(
                sql.Identifier(schema),
                sql.Identifier(name),
                sql.Identifier(column),
                sql.Placeholder()
            ),
            params=(datatype,)
        )

        return sequel
# --------------------------------------------------------------------------- #
#                              SCHEMA SEQUEL                                  #
# --------------------------------------------------------------------------- #


class SchemaSequel(AdminSequelBase):

    def create(self, name: str) -> Sequel:
        sequel = Sequel(
            name="create_schema",
            description="Created schema {}".format(name),
            cmd=sql.SQL("CREATE SCHEMA {} ;").format(
                sql.Identifier(name))
        )

        return sequel

    def drop(self, name: str) -> Sequel:
        sequel = Sequel(
            name="drop_schema",
            description="Dropped schema {}".format(name),
            cmd=sql.SQL("DROP SCHEMA {} CASCADE;").format(
                sql.Identifier(name))
        )

        return sequel

    def exists(self, name: str) -> Sequel:
        subquery = sql.SQL("""SELECT 1 FROM information_schema.schemata
                                WHERE schema_name = {}""").format(
            sql.Identifier(name)
        )
        sequel = Sequel(
            name="schema_exists",
            description="Checked existence of schema {}".format(name),
            cmd=sql.SQL("SELECT EXISTS({});").format(subquery)
        )

        return sequel


# =========================================================================== #
#                               ACCESS QUERIES                                #
# =========================================================================== #
class AccessSequel(AccessSequelBase):

    def _get(self, table: str, schema: str, columns: list = None,
             where_key: str = None,
             where_value: Union[str, int, float] = None)\
            -> Sequel:

        sequel = Sequel(
            name="select",
            description="Selected {} from {}.{} where {} = {}".format(
                columns, schema, table, where_key, where_value
            ),
            cmd=sql.SQL("SELECT {} FROM {}.{} WHERE {} = {};").format(
                sql.SQL(", ").join(map(sql.Identifier, columns)),
                sql.Identifier(schema),
                sql.Identifier(table),
                sql.Identifier(where_key),
                sql.Placeholder()),
            params=(where_value,)
        )
        return sequel

    def _get_all_columns_all_rows(self, table: str, schema: str,
                                  columns: list = None,
                                  where_key: str = None,
                                  where_value: Union[str, int, float] = None)\
            -> Sequel:

        sequel = Sequel(
            name="select",
            description="Selected * from {}.{}".format(
                schema, table
            ),
            cmd=sql.SQL("SELECT * FROM {}.{};").format(
                sql.Identifier(schema),
                sql.Identifier(table)
            )
        )
        return sequel

    def _get_all_rows(self, table: str, schema: str, columns: list = None,
                      where_key: str = None,
                      where_value: Union[str, int, float] = None) -> Sequel:

        sequel = Sequel(
            name="select",
            description="Selected {} from {}.{}".format(
                columns, schema, table
            ),
            cmd=sql.SQL("SELECT {} FROM {}.{};").format(
                sql.SQL(", ").join(map(sql.Identifier, columns)),
                sql.Identifier(schema),
                sql.Identifier(table)
            )
        )
        return sequel

    def _get_all_columns(self, table: str, schema: str,
                         columns: list = None,
                         where_key: str = None,
                         where_value: Union[str, int, float] = None)\
            -> Sequel:

        sequel = Sequel(
            name="select",
            description="Selected * from {}.{} where {} = {}".format(
                schema, table, where_key, where_value
            ),
            cmd=sql.SQL("SELECT * FROM {}.{} WHERE {} = {};").format(
                sql.Identifier(schema),
                sql.Identifier(table),
                sql.Identifier(where_key),
                sql.Placeholder()
            ),
            params=(where_value,)
        )
        return sequel

    def get(self, table: str, schema: str, columns: list = None,
            where_key: str = None, where_value: Union[str, int, float] = None)\
            -> Sequel:

        if (where_key is None and where_value is None) != \
                (where_key is None or where_value is None):
            raise ValueError("where values not completely specified.")

        if (columns is not None and where_key is not None):
            # Returns selected columns from selected rows
            return self._get(table=table, schema=schema, columns=columns,
                             where_key=where_key, where_value=where_value)
        elif (columns is not None):
            # Returns all rows, selected columns
            return self._get_all_rows(table=table, schema=schema,
                                      columns=columns,
                                      where_key=where_key,
                                      where_value=where_value)

        elif (where_key is not None):
            # Returns all columns, selected rows
            return self._get_all_columns(table=table, schema=schema,
                                         columns=columns,
                                         where_key=where_key,
                                         where_value=where_value)

        else:
            return self._get_all_columns_all_rows(table=table, schema=schema,
                                                  columns=columns,
                                                  where_key=where_key,
                                                  where_value=where_value)

    def add(self, table: str, schema: str, columns: list,
            values: list) -> Sequel:

        if (len(columns) != len(values)):
            raise ValueError(
                "Number of columns doesn't match number of values")

        sequel = Sequel(
            name="insert",
            description="Inserted into {}.{} {} values {}".format(
                schema, table, columns, table
            ),
            cmd=sql.SQL("INSERT into {}.{} ({}) values ({});")
            .format(
                sql.Identifier(schema),
                sql.Identifier(table),
                sql.SQL(', ').join(map(sql.Identifier, tuple((*columns,)))),
                sql.SQL(', ').join(sql.Placeholder() * len(columns))
            ),
            params=(*values,)
        )

        return sequel

    def update(self, table: str, schema: str, column: str,
               value: Union[str, float, int], where_key: str,
               where_value: Union[str, float, int]) -> Sequel:

        sequel = Sequel(
            name="update",
            description="Updated {}.{} setting {} = {} where {} = {}".format(
                schema, table, column, value, where_key, where_value
            ),
            cmd=sql.SQL("UPDATE {}.{} SET {} = {} WHERE {} = {}").format(
                sql.Identifier(schema),
                sql.Identifier(table),
                sql.Identifier(column),
                sql.Placeholder(),
                sql.Identifier(where_key),
                sql.Placeholder()
            ),
            params=(value, where_value,)
        )

        return sequel

    def delete(self, table: str, schema: str, where_key: str,
               where_value: Union[str, float, int]) -> Sequel:

        sequel = Sequel(
            name="delete",
            description="Deleted from {}.{} where {} = {}".format(
                schema, table, where_key, where_value
            ),
            cmd=sql.SQL("DELETE FROM {} WHERE {} = {}").format(
                sql.Identifier(table),
                sql.Identifier(where_key),
                sql.Placeholder()
            ),
            params=(where_value,)
        )

        return sequel
