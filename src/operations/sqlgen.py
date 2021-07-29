#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# =========================================================================== #
# Project  : Drug Approval Analytics                                          #
# Version  : 0.1.0                                                            #
# File     : \src\data\database\sqlgen.py                               #
# Language : Python 3.9.5                                                     #
# --------------------------------------------------------------------------  #
# Author   : John James                                                       #
# Company  : nov8.ai                                                          #
# Email    : john.james@nov8.ai                                               #
# URL      : https://github.com/john-james-sf/drug-approval-analytics         #
# --------------------------------------------------------------------------  #
# Created  : Monday, July 19th 2021, 2:26:36 pm                               #
# Modified : Thursday, July 29th 2021, 6:20:14 am                             #
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
#                                 USER ADMIN                                  #
# --------------------------------------------------------------------------- #
class CreateUser(QueryBuilder):

    def build(self, credentials: dict, create_db: bool = True)\
            -> SQLCommand:

        if create_db:
            description = "CREATE USER {} WITH PASSWORD {} CREATEDB;".format(
                credentials['user'], '*****')
            cmd = sql.SQL("CREATE USER {} WITH PASSWORD {} CREATEDB;").format(
                sql.Identifier(credentials['user']),
                sql.Placeholder())
        else:
            description = "CREATE USER {} WITH PASSWORD {} NOCREATEDB;".format(
                credentials['user'], '*****')
            cmd = sql.SQL("CREATE USER {} WITH PASSWORD {} NOCREATEDB;")\
                .format(sql.Identifier(credentials['user']),
                        sql.Placeholder())
        command = \
            SQLCommand(
                name="create_user", description=description, cmd=cmd,
                params=(credentials['password'],))

        return command


# -----------------------------------------------------------------------------#
class DropUser(QueryBuilder):

    def build(self, name: str) -> SQLCommand:

        command = SQLCommand(
            name="drop_user",
            description="Drop USER {}.".format(name),
            cmd=sql.SQL("DROP USER IF EXISTS {} ;").format(
                sql.Identifier(name)))

        return command


# -----------------------------------------------------------------------------#
class UserExists(QueryBuilder):

    def build(self, name: str) -> SQLCommand:

        command = SQLCommand(
            name="user_exists",
            description="Determines if user {} exists.;".format(
                name),
            cmd=sql.SQL("""SELECT EXISTS(
                    SELECT usename FROM pg_catalog.pg_user
                    WHERE lower(usename) = lower(%s));"""),
            params=(name,)
        )

        return command


# --------------------------------------------------------------------------- #
#                               DATABASE ADMIN                                #
# --------------------------------------------------------------------------- #
class CreateDatabase(QueryBuilder):

    def build(self, name: str) -> SQLCommand:

        command = SQLCommand(
            name="create",
            description="Create {} database".format(name),
            cmd=sql.SQL("CREATE DATABASE {} ;").format(
                sql.Identifier(name)))

        return command


# -----------------------------------------------------------------------------#
class GrantPrivileges(QueryBuilder):

    def build(self, name: str, user: str) -> SQLCommand:

        command = SQLCommand(
            name="grant",
            description="Grant privileges on database {} to {}"
            .format(name, user),
            cmd=sql.SQL("GRANT ALL PRIVILEGES ON DATABASE {} TO {} ;")
            .format(
                sql.Identifier(name),
                sql.Identifier(user)))

        return command


# -----------------------------------------------------------------------------#
class DropDatabase(QueryBuilder):

    def build(self, name: str) -> SQLCommand:

        command = SQLCommand(
            name="drop database",
            description="Drop {} database if it exists.".format(name),
            cmd=sql.SQL("DROP DATABASE IF EXISTS {} ;").format(
                sql.Identifier(name)))

        return command


# --------------------------------------------------------------------------- #
class DatabaseRename(QueryBuilder):

    def build(self, name: str, newname: str) -> SQLCommand:

        command = SQLCommand(
            name="database exists",
            description="Check existence of {} database.".format(name),
            cmd=sql.SQL("ALTER DATABASE {} RENAME TO {};".format(
                sql.Identifier(name),
                sql.Identifier(newname)
            )))

        return command

# --------------------------------------------------------------------------- #


class DatabaseExists(QueryBuilder):

    def build(self, name: str) -> SQLCommand:

        command = SQLCommand(
            name="database exists",
            description="Check existence of {} database.".format(name),
            cmd=sql.SQL("""SELECT EXISTS(
                    SELECT datname FROM pg_catalog.pg_database
                    WHERE lower(datname) = lower(%s));"""),
            params=(name,))

        return command


# --------------------------------------------------------------------------- #
class DatabaseStats(QueryBuilder):

    def build(self) -> SQLCommand:

        command = SQLCommand(
            name="database_stats",
            description="Database stats for each table in the \
                current database",
            cmd=sql.SQL("""SELECT schemaname, relname, n_live_tup \
                FROM pg_stat_user_tables
                            ORDER BY schemaname, relname;""")
        )

        return command


# --------------------------------------------------------------------------- #
#                              TABLE ADMIN                                    #
# --------------------------------------------------------------------------- #
class TableExists(QueryBuilder):
    """Builds a query that evaluates existence of a table."""

    def build(self, name: str, dbname: str) -> SQLCommand:
        command = SQLCommand(
            name='table_exists',
            description="Evaluating existance of {name}".format(
                name=sql.Identifier(name)),
            cmd=sql.SQL("SELECT EXISTS(SELECT 1 FROM information_schema.tables\
                WHERE table_catalog = {dbname} AND \
                    table_schema = PUBLIC AND \
                    table_name = {name});").format(
                dbname=sql.Placeholder(dbname),
                name=sql.Placeholder(name)),
            params=tuple(name, dbname))

        return command


# -----------------------------------------------------------------------------#
class ColumnInserter(QueryBuilder):
    """Builds a query to insert one or more columns into a table."""

    def build(self, name: str, columns, list) -> SQLCommand:

        command = SQLCommand(
            name='add_columns',
            description="Adding column(s) to {name}".format(name=name),
            cmd=sql.SQL("ALTER TABLE {table} {columns}".format(
                table=sql.Identifier(name),
                columns=sql.Identifier(sql.SQL(", ").join(
                    sql.SQL("ADD COLUMN IF NOT EXISTS\
                            {column} {datatype}".format(
                        column=name,
                        datatype=datatype)
                        for name, datatype in columns.items())
                )
                )
            )
            )
        )

        return command


# -----------------------------------------------------------------------------#
class ColumnExists(QueryBuilder):
    """Builds a query to insert one or more columns into a table."""

    def build(self, name: str, column: str) -> SQLCommand:

        command = SQLCommand(
            name='add_columns',
            description="Adding column(s) to {name}".format(name=name),
            cmd=sql.SQL("SELECT EXISTS (SELECT 1 \
            FROM information_schema.columns \
            WHERE table_name = {table} AND column_name = {column});".format(
                table=sql.Placeholder(),
                column=sql.Placeholder())
            ),
            params=(name, column,)
        )

        return command


# -----------------------------------------------------------------------------#
class ColumnRemover(QueryBuilder):
    """Builds a query to remove one or more columns from a table."""

    def build(self, name: str, columns, list) -> SQLCommand:

        command = SQLCommand(
            name="column_remove",
            description="Column Remover",
            cmd=sql.SQL("ALTER TABLE {table} {cols}; ").format(
                table=sql.Identifier(name),
                cols=sql.SQL(", ").join(
                    sql.Identifier(
                        sql.SQL("DROP COLUMN {c}").format(c=c)
                        for c in columns))))

        return command


# -----------------------------------------------------------------------------#
class DropTable(QueryBuilder):

    def build(self, name: str) -> SQLCommand:

        command = SQLCommand(
            name="drop_table",
            description="Drop {} table if it exists.".format(name),
            cmd=sql.SQL("DROP TABLE IF EXISTS {};").format(
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
