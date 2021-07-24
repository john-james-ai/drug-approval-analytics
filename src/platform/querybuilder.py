#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# =========================================================================== #
# Project  : Drug Approval Analytics                                          #
# Version  : 0.1.0                                                            #
# File     : \src\data\database\querybuilder.py                               #
# Language : Python 3.9.5                                                     #
# --------------------------------------------------------------------------  #
# Author   : John James                                                       #
# Company  : nov8.ai                                                          #
# Email    : john.james@nov8.ai                                               #
# URL      : https://github.com/john-james-sf/drug-approval-analytics         #
# --------------------------------------------------------------------------  #
# Created  : Monday, July 19th 2021, 2:26:36 pm                               #
# Modified : Friday, July 23rd 2021, 10:11:46 pm                              #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
"""Query Template Generator."""
from abc import ABC, abstractmethod
from psycopg2 import sql
from ...utils.logger import logger
from .database import SQLCommand
# -----------------------------------------------------------------------------#


class QueryBuilder(ABC):
    """Generates sql query templates for pyscopg2 SQL statements. """

    @abstractmethod
    def build(self, name: str, *args, **kwargs):
        pass

# --------------------------------------------------------------------------- #
#                          DATABASE ADMIN                                     #
# --------------------------------------------------------------------------- #


class CreateDatabase(QueryBuilder):

    def build(self, name: str) -> SQLCommand:

        command =\
            SQLCommand(
                name="create",
                description="Create {} database".format(name),
                cmd=sql.SQL("CREATE DATABASE {} ;").format(
                    sql.Identifier(self._name)))

        return command


# -----------------------------------------------------------------------------#
class GrantPrivileges(QueryBuilder):

    def build(self, name: str, credentials: dict) -> SQLCommand:

        command =\
            SQLCommand(
                name="grant",
                description="Grant privileges on database {} to {}"
                .format(name, self._credentials['user']),
                cmd=sql.SQL("GRANT ALL PRIVILEGES ON DATABASE {} TO {} ;")
                .format(
                    sql.Identifier(name),
                    sql.Identifier(credentials['user'])))

        return command

# -----------------------------------------------------------------------------#


class DropDatabase(QueryBuilder):

    def build(self, name: str) -> SQLCommand:

        command = \
            SQLCommand(
                name="drop database",
                description="Drop {} database if it exists.".format(name),
                cmd=sql.SQL("DROP DATABASE IF EXISTS {} ;").format(
                    sql.Identifier(name)))

        return command

# --------------------------------------------------------------------------- #


class DatabaseExists(QueryBuilder):

    def build(self, name: str) -> SQLCommand:

        command = \
            SQLCommand(
                name="database exists",
                description="Check existence of {} database.".format(name),
                cmd=sql.SQL("""SELECT EXISTS(
                    SELECT datname FROM pg_catalog.pg_database
                    WHERE lower(datname) = lower(%s));"""),
                params=(name,))

        return command
# --------------------------------------------------------------------------- #


class CreateTable(QueryBuilder):

    @ property
    def tablename(self) -> str:
        return self._name

    @ tablename.setter
    def tablename(self, name) -> None:
        self._name - name

    def add_column(self, name, datatype, nullable=True,
                   unique=False, default=None):
        self._columns[name] = {}
        self._columns[name]['datatype'] = datatype
        self._columns[name]['nullable'] = nullable
        self._columns[name]['unique'] = unique
        self._columns[name]['default'] = default
        return self

    def set_primary_key(self, columns=None):
        if columns:
            if isinstance(columns, str):
                self._primary_key.append(columns)
            elif isinstance(columns, list):
                self._primary_key = columns
            else:
                logger.error(
                    "Primary key must be a string column name \
                        or list of column names")
                raise TypeError

    def set_foreign_key(self, name, reftable, refcolumn):
        self._foreign_keys[name] = {}
        self._foreign_keys[name]['reftable'] = reftable
        self._foreign_keys[name]['refcolumn'] = refcolumn
        return self

    def _get_command(self):
        self._sql_command = "CREATE TABLE {} (".format(self._name)

    def _get_columns(self):
        cols = []
        for name, properties in self._columns.items():
            column_properties = []
            column_properties.append(
                "{} {}".format(name, properties['datatype']))
            if not properties['nullable']:
                column_properties.append("NOT NULL")
            if properties['unique']:
                column_properties.append("UNIQUE")
            column_properties = " ".join(column_properties)
            cols.append(column_properties)
        self._sql_command += ", ".join(cols)

    def _get_primary_key(self):
        if len(self._primary_key) > 0:
            pk = ", ".join(self._primary_key)
            self._sql_command += "PRIMARY KEY ({})".format(pk)

    def _get_foreign_key(self):
        if len(self._foreign_keys) > 0:
            for name, params in self._foreign_keys.items():
                self._sql_command += "CONSTRAINT {} ".format(name)
                self._sql_command += "FOREIGN KEY ({}) ".format(
                    params['refcolumn'])
                self._sql_command += "REFERENCES {}({}) ".format(
                    params['reftable'], params['refcolumn'])

    def _get_closing(self):
        self._sql_command += ");"

    def build(self, name: str, schema: list) -> SQLCommand:
        self._get_command()
        self._get_columns()
        self._get_primary_key()
        self._get_foreign_key()
        self._get_closing()
        return sql.SQL(self._sql_command)


# -----------------------------------------------------------------------------#
class ColumnInserter(QueryBuilder):
    """Builds a query to insert one or more columns into a table."""

    def build(self, name: str, schema: str, columns, dict) -> SQLCommand:

        command = \
            sql.SQL("ALTER TABLE {schema}.{table} {cols}; ").format(
                schema=schema,
                table=sql.Identifier(name),
                cols=sql.SQL(", ").join(
                    sql.Identifier(
                        sql.SQL("ADD {c}").format(c=c)
                        for c in columns.values())))

        return command


# -----------------------------------------------------------------------------#
class ColumnRemover(QueryBuilder):
    """Builds a query to remove one or more columns from a table."""

    def build(self, name: str, schema: str, columns, dict) -> SQLCommand:

        command = \
            sql.SQL("ALTER TABLE {schema}.{table} {cols}; ").format(
                schema=schema,
                table=sql.Identifier(name),
                cols=sql.SQL(", ").join(
                    sql.Identifier(
                        sql.SQL("DROP COLUMN {c}").format(c=c)
                        for c in columns.keys())))

        return command


# -----------------------------------------------------------------------------#
class DropTable(QueryBuilder):

    def build(self, name: str) -> SQLCommand:

        command = SQLCommand(
            name="drop database",
            description="Drop {} table if it exists.".format(name),
            cmd=sql.SQL("DROP TABLE IF EXISTS {} ;").format(
                sql.Identifier(name)))

        return command
