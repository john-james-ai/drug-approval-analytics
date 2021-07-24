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
# Modified : Saturday, July 24th 2021, 5:21:42 am                             #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
"""Query Template Generator."""
from abc import ABC, abstractmethod
from psycopg2 import sql
from dataclasses import dataclass, field
from datetime import datetime
# -----------------------------------------------------------------------------#


class QueryBuilder(ABC):
    """Generates sql query templates for pyscopg2 SQL statements. """

    @abstractmethod
    def build(self, name: str, *args, **kwargs):
        pass
# --------------------------------------------------------------------------- #
#                              SQL COMMAND                                    #
# --------------------------------------------------------------------------- #


@dataclass
class SQLCommand:
    """Class that encapsulates a sql command, its name and parameters."""
    name: str
    cmd: sql
    description: field(default=None)
    params: tuple = field(default_factory=tuple)
    executed: datetime = field(default=datetime(1970, 1, 1, 0, 0))

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
                    sql.Identifier(name)))

        return command


# -----------------------------------------------------------------------------#
class GrantPrivileges(QueryBuilder):

    def build(self, name: str, credentials: dict) -> SQLCommand:

        command =\
            SQLCommand(
                name="grant",
                description="Grant privileges on database {} to {}"
                .format(name, self._credentials['user']),
                cmd=sql.SQL("GRANT ALL PRIVILEGES ON DATABASE {} TO PUBLIC ;")
                .format(
                    sql.Identifier(name)))

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
#                              TABLE ADMIN                                    #
# --------------------------------------------------------------------------- #
class CreateTable(QueryBuilder):

    def build(self, name: str, schema: dict, columns: dict) -> SQLCommand:

        # Create columns list and combine it into a single string
        cols = []
        for name, constraint in columns.items():
            column = sql.SQL("{name} {constraint}").format(
                name=sql.Identifier(name),
                constraint=sql.Identifier(constraint))
            cols.append(column)
        expression = sql.SQL(", ").join(cols)

        query = sql.SQL("CREATE TABLE {schema}.{table} ({columns});").format(
            schema=sql.Identifier(schema),
            table=sql.Identifier(name),
            columns=sql.Identifier(expression))

        command = SQLCommand(
            name="create_table",
            description="Create {}.{} table".format(schema, name),
            cmd=query)

        return command

# -----------------------------------------------------------------------------#


class TableExists(QueryBuilder):
    """Builds a query that evaluates existence of a table."""

    def build(self, name: str, schema: str, dbname: str) -> SQLCommand:
        command = SQLCommand(
            name='table_exists',
            description="Evaluating existance of to {name}.{schema}".format(
                name=sql.Identifier(name),
                schema=sql.Identifier(schema)),
            cmd=sql.SQL("SELECT EXISTS(SELECT 1 FROM information_schema.tables\
                WHERE table_catalog = {dbname} AND \
                    table_schema = {schema} AND \
                    table_name = {name});").format(
                dbname=sql.Placeholder(dbname),
                schema=sql.Placeholder(schema),
                name=sql.Placeholder(name)),
            params=tuple(name, schema, dbname))

        return command

# -----------------------------------------------------------------------------#


class ColumnInserter(QueryBuilder):
    """Builds a query to insert one or more columns into a table."""

    def build(self, name: str, schema: str, columns, dict) -> SQLCommand:

        command = SQLCommand(
            name='add_column',
            description="Adding column(s) to {name}".format(name=name),
            cmd=sql.SQL("ALTER TABLE {schema}.{table} {cols}; ").format(
                schema=schema,
                table=sql.Identifier(name),
                cols=sql.SQL(", ").join(
                    sql.Identifier(
                        sql.SQL("ADD {c}").format(c=c)
                        for c in columns.values()))))

        return command


# -----------------------------------------------------------------------------#
class ColumnRemover(QueryBuilder):
    """Builds a query to remove one or more columns from a table."""

    def build(self, name: str, schema: str, columns, dict) -> SQLCommand:

        command = SQLCommand(
            name="column_remove",
            description="Column Remover",
            cmd=sql.SQL("ALTER TABLE {schema}.{table} {cols}; ").format(
                schema=schema,
                table=sql.Identifier(name),
                cols=sql.SQL(", ").join(
                    sql.Identifier(
                        sql.SQL("DROP COLUMN {c}").format(c=c)
                        for c in columns.keys()))))

        return command


# -----------------------------------------------------------------------------#
class DropTable(QueryBuilder):

    def build(self, name: str, schema: str) -> SQLCommand:

        command = SQLCommand(
            name="drop_table",
            description="Drop {} table if it exists.".format(name),
            cmd=sql.SQL("DROP TABLE IF EXISTS {}.{} ;").format(
                sql.Identifier(schema),
                sql.Identifier(name)))

        return command


# -----------------------------------------------------------------------------#
class SimpleQuery(QueryBuilder):

    def build(self, name: str, tablename: str, schema: str, columns: list,
              keys: list, comparators: list, operators: list,
              values: list) -> SQLCommand:

        # Validate to ensure keys, operators, and values are same length
        length = len(keys)
        if any(len(element) != length for element in
               [keys, comparators, operators, values]):
            raise ValueError(
                "key, operator, and values must be equal length lists.")

        # Add an empty character to end of operators so that we don't
        # have to deal with the 1- problem
        operators += ""

        # Create a list of where clauses
        conditions = {}
        for key, comparator, operator in \
                zip(keys, comparators, operators):
            condition = sql.SQL("{key} {compare} {value} {oper}").format(
                key=sql.Identifier(key),
                compare=sql.Identifier(comparator),
                value=sql.Placeholder(),
                oper=sql.Identifier(operator))
            conditions.append(condition)

        # Merge the list of conditions into a single string.
        where_clause = sql.SQL("WHERE {clause}").format(
            clause=sql.SQL(" ").join(map(sql.Identifer, conditions))
        )

        # Not sure if this will work.
        query = sql.SQL("SELECT {fields} FROM {schema}.{table} {where}".format(
            fields=sql.SQL(",").join(map(sql.Identifier(columns))),
            schema=sql.Identifier(schema),
            table=sql.Identifier(tablename),
            where=sql.Identifier(where_clause)))

        # Let's pack it up and see what we have.
        command = SQLCommand(
            NAME='simple_query',
            description='Simple Query on {} table'.format(tablename),
            cmd=query,
            params=tuple(values))

        return command
