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
# Modified : Saturday, July 31st 2021, 2:12:56 am                             #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
"""SQL Generator.

Module contains SQL generator classes for three primary contexts:

    - Database Administration: Including user, database and table management.
    - Operations Repository Administration: SQL to create the repository of
        artifacts (data sources, datasets, machine learning
        parameters) and events (execution of pipelines and pipeline steps).
    - Operations Repository Usage: Basic SQL to support rudimentary CRUD
        operations on artifacts and events.

All queries are returned in a standard SQLCommand dataclass that bundles
the sql with basic descriptive metadata and associated SQL command parameters.
This information is provided for logging, operations and pipeline management.

"""
from abc import ABC, abstractmethod
from typing import Union
from psycopg2 import sql
from dataclasses import dataclass, field
from datetime import datetime
# --------------------------------------------------------------------------- #
#                              SQL COMMAND                                    #
# --------------------------------------------------------------------------- #


@dataclass
class SQLCommand:
    """Class that encapsulates a sql command, its name and parameters."""
    name: str
    cmd: sql
    description: str = field(default=None)
    params: tuple = field(default=())
    executed: datetime = field(default=datetime(1970, 1, 1, 0, 0))


# --------------------------------------------------------------------------- #
#                             QUERY BUILDER                                   #
# --------------------------------------------------------------------------- #
class QueryBuilder(ABC):
    """Abstract base class for concrete builder subclasses."""

    @abstractmethod
    def build(self, name: str, *args, **kwargs):
        pass
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
                params=tuple((credentials['password'],))
            )

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
            params=tuple((name,))
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


class RevokePrivileges(QueryBuilder):

    def build(self, name: str, user: str) -> SQLCommand:

        command = SQLCommand(
            name="revoke",
            description="Revoke privileges on database {} FROM {}"
            .format(name, user),
            cmd=sql.SQL("REVOKE ALL PRIVILEGES ON DATABASE {} FROM {} ;")
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
            params=tuple((name,))
        )

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

    def build(self, name: str, schema: str = "PUBLIC") -> SQLCommand:
        command = SQLCommand(
            name='table_exists',
            description="Evaluating existance of {name}".format(
                name=name),
            cmd=sql.SQL("SELECT TRUE FROM information_schema.tables\
                WHERE table_name = {}").format(
                sql.Placeholder()),
            params=tuple((name,))
        )

        return command


# --------------------------------------------------------------------------- #
class ColumnExists(QueryBuilder):
    """Builds a query to insert one or more columns into a table."""

    def build(self, name: str, table: str, schema: str = 'PUBLIC')\
            -> SQLCommand:

        command = SQLCommand(
            name='column_exists',
            description="Check if column {} exist on table {}".format(
                name, table),
            cmd=sql.SQL("SELECT EXISTS (SELECT 1 \
            FROM information_schema.columns \
            WHERE table_schema = {} AND table_name = {} AND \
                column_name = {});".format(
                sql.Placeholder(),
                sql.Placeholder(),
                sql.Placeholder())),
            params=tuple((schema, table, name,))
        )

        return command


# --------------------------------------------------------------------------- #
class DropTable(QueryBuilder):

    def build(self, name: str, cascade: bool = False) -> SQLCommand:

        if cascade:
            cmd = sql.SQL("DROP TABLE IF EXISTS {} CASCADE;").format(
                sql.Identifier(name))
        else:
            cmd = sql.SQL("DROP TABLE IF EXISTS {};").format(
                sql.Identifier(name))

        command = SQLCommand(
            name="drop_table",
            description="Drop {} table if it exists.".format(name),
            cmd=cmd)

        return command

# =========================================================================== #
#                              REPOSITORY                                     #
# =========================================================================== #
# --------------------------------------------------------------------------- #
#                            ARTIFACT TABLE                                   #
# --------------------------------------------------------------------------- #


class CreateArtifactTable(QueryBuilder):
    """Generates SQL to create the Artifact table."""

    def build(self) -> SQLCommand:
        command = SQLCommand(
            name="artifact",
            description="Create artifact table.",
            cmd=sql.SQL("""
                CREATE TABLE IF NOT EXISTS artifact (
                    id INTEGER GENERATED BY DEFAULT AS IDENTITY,
                    name VARCHAR(64) UNIQUE NOT NULL,
                    title VARCHAR(64),
                    version INTEGER,
                    type VARCHAR(32),
                    description VARCHAR(256),
                    creator VARCHAR(128),
                    maintainer VARCHAR(128),
                    webpage VARCHAR(255),
                    uri VARCHAR(255),
                    uri_type VARCHAR(32),
                    media_type VARCHAR(32),
                    frequency VARCHAR(32),
                    coverage VARCHAR(64),
                    lifecycle INTEGER,
                    created TIMESTAMP WITH TIME ZONE NOT NULL
                        DEFAULT CURRENT_TIMESTAMP,
                    updated TIMESTAMP,
                    extracted TIMESTAMP
                );"""
                        )
        )
        return command

# --------------------------------------------------------------------------- #
#                          CREATE ENTITY TABLE                                #
# --------------------------------------------------------------------------- #


class CreateEntityTable(QueryBuilder):
    """Generates SQL to create the Entity table."""

    def build(self) -> SQLCommand:
        command = SQLCommand(
            name="entity",
            description="Create entity table.",
            cmd=sql.SQL("""
                CREATE TABLE IF NOT EXISTS feature (
                    id INTEGER GENERATED BY DEFAULT AS IDENTITY,
                    name VARCHAR(64) UNIQUE NOT NULL,
                    version INTEGER,
                    description VARCHAR(256),
                    kind VARCHAR(32) NOT NULL,
                    datatype VARCHAR(32) NOT NULL,
                    da_samples INTEGER,
                    uri VARCHAR(255),
                    uri_type VARCHAR(32),
                    colname VARCHAR(128),
                    media_type VARCHAR(32),
                    created TIMESTAMP WITH TIME ZONE NOT NULL
                        DEFAULT CURRENT_TIMESTAMP,
                    updated TIMESTAMP
                );"""
                        )
        )
        return command
# --------------------------------------------------------------------------- #
#                          CREATE FEATURE TABLE                               #
# --------------------------------------------------------------------------- #


class CreateFeatureTable(QueryBuilder):
    """Generates SQL to create the Artifact table."""

    def build(self) -> SQLCommand:
        command = SQLCommand(
            name="feature",
            description="Create feature table.",
            cmd=sql.SQL("""
                CREATE TABLE IF NOT EXISTS feature (
                    id INTEGER GENERATED BY DEFAULT AS IDENTITY,
                    name VARCHAR(64) UNIQUE NOT NULL,
                    version INTEGER,
                    description VARCHAR(256),
                    entity_id INTEGER REFERENCES entity (id),
                    kind VARCHAR(32) NOT NULL,
                    datatype VARCHAR(32) NOT NULL,
                    da_samples INTEGER,
                    dan_min FLOAT,
                    dan_max FLOAT,
                    dan_q1 FLOAT,
                    dan_mean FLOAT,
                    dan_median FLOAT,
                    dan_q3 FLOAT,
                    dan_var FLOAT,
                    dan_sd FLOAT,
                    dan_skew FLOAT,
                    dan_kurtosis FLOAT,
                    dan_num_null INTEGER,
                    dan_pct_null FLOAT GENERATED ALWAYS AS
                        (dan_num_null / da_samples * 100),
                    dan_num_inf INTEGER,
                    dan_pct_inf FLOAT GENERATED ALWAYS AS
                        (dan_num_inf / da_samples * 100),
                    dan_num_zero INTEGER,
                    dan_pct_zero FLOAT GENERATED ALWAYS AS
                        (dan_num_zero / da_samples * 100),
                    dac_num_unique INTEGER,
                    dac_pct_unique FLOAT GENERATED ALWAYS AS
                        (dan_num_unique / da_samples * 100),
                    dac_rank1 VARCHAR(256),
                    dac_rank1_freq INTEGER,
                    dac_rank1_pct FLOAT GENERATED ALWAYS AS
                        (dac_rank1_freq / da_samples * 100),
                    uri VARCHAR(255),
                    uri_type VARCHAR(32),
                    colname VARCHAR(128),
                    media_type VARCHAR(32),
                    created TIMESTAMP WITH TIME ZONE NOT NULL
                        DEFAULT CURRENT_TIMESTAMP,
                    updated TIMESTAMP
                );"""
                        )
        )
        return command

# --------------------------------------------------------------------------- #
#                        CREATE PIPELINE TABLE                                #
# --------------------------------------------------------------------------- #


class CreatePipelineTable(QueryBuilder):
    """Generates SQL to create the Parameter table."""

    def build(self) -> SQLCommand:
        command = SQLCommand(
            name="pipeline",
            description="Create pipeline table.",
            cmd=sql.SQL("""
                CREATE TABLE IF NOT EXISTS pipeline (
                    id INTEGER GENERATED BY DEFAULT AS IDENTITY,
                    name VARCHAR(64) UNIQUE NOT NULL,
                    description VARCHAR(256),
                    return_code INTEGER,
                    return_value VARCHAR(64),
                    created TIMESTAMP WITH TIME ZONE NOT NULL
                        DEFAULT CURRENT_TIMESTAMP,
                    updated TIMESTAMP,
                    executed TIMESTAMP,
                );"""
                        )
        )
        return command

# --------------------------------------------------------------------------- #
#                           CREATE EVENT TABLE                                #
# --------------------------------------------------------------------------- #


class CreateEventTable(QueryBuilder):
    """Generates SQL to create the EVENT table."""

    def build(self) -> SQLCommand:
        command = SQLCommand(
            name="event",
            description="Create event table.",
            cmd=sql.SQL("""
                CREATE TABLE IF NOT EXISTS event (
                    id INTEGER GENERATED BY DEFAULT AS IDENTITY,
                    name VARCHAR(64) UNIQUE NOT NULL,
                    description VARCHAR(256),
                    input_id INTEGER REFERENCES artifact (id),
                    output_id INTEGER REFERENCES artifact (id),
                    pipeline_id INTEGER REFERENCES pipeline (id),
                    return_code INTEGER,
                    return_value VARCHAR(64),
                    created TIMESTAMP WITH TIME ZONE NOT NULL
                        DEFAULT CURRENT_TIMESTAMP,
                    start TIMESTAMP,
                    end TIMESTAMP,
                );"""
                        )
        )
        return command


# --------------------------------------------------------------------------- #
#                        CREATE PARAMETER TABLE                               #
# --------------------------------------------------------------------------- #
class CreateParameterTable(QueryBuilder):
    """Generates SQL to create the Parameter table."""

    def build(self) -> SQLCommand:
        command = SQLCommand(
            name="parameter",
            description="Create parameter table.",
            cmd=sql.SQL("""
                CREATE TABLE IF NOT EXISTS step (
                    id INTEGER GENERATED BY DEFAULT AS IDENTITY,
                    name VARCHAR(64) UNIQUE NOT NULL,
                    description VARCHAR(256),
                    numeric_value FLOAT,
                    string_value VARCHAR(32),
                    algorithm VARCHAR(64),
                    event_id INTEGER REFERENCES event (id),
                    created TIMESTAMP WITH TIME ZONE NOT NULL
                        DEFAULT CURRENT_TIMESTAMP,
                    updated TIMESTAMP,
                    executed TIMESTAMP,
                );"""
                        )
        )
        return command


# --------------------------------------------------------------------------- #
#                                 SELECT                                      #
# --------------------------------------------------------------------------- #
class Select(QueryBuilder):
    """Generates SQL to support basic queries."""

    def _validate(self, table: str, columns: list = None,
                  where_key: str = None,
                  where_value: Union[str, int, float] = None) -> SQLCommand:

        if (where_key and where_value) !=\
                (where_key or where_value):
            raise ValueError("where values not completely specified.")

    def _build_cmd(self, table: str, columns: list = None) -> SQLCommand:

        return sql.SQL("SELECT {} FROM {}").format(
            sql.SQL(", ").join(map(sql.Identifier, columns)),
            sql.Identifier(table))

    def _build_where_cmd(self, table: str, columns: list = None,
                         where_key: str = None,
                         where_value: Union[str, int, float] = None)\
            -> SQLCommand:

        return sql.SQL("SELECT {} FROM {} WHERE {} = {} ").format(
            sql.SQL(", ").join(map(sql.Identifier, columns)),
            sql.Identifier(table),
            sql.Identifier(where_key),
            sql.Identifier(where_value))

    def build(self, table: str, columns: list = None, where_key: str = None,
              where_value: Union[str, int, float] = None) -> SQLCommand:

        self._validate(table, columns, where_key, where_value)

        columns = columns if columns is not None else ['*']

        if where_key and where_value:
            cmd = self._build_where_cmd(table, columns, where_key, where_value)
        else:
            cmd = self._build_cmd(table, columns)

        command = SQLCommand(
            name="select",
            description="Select from {}.".format(table),
            cmd=cmd)

        return command

# --------------------------------------------------------------------------- #
#                                 INSERT                                      #
# --------------------------------------------------------------------------- #


class Insert(QueryBuilder):
    """Generates SQL to support insert commands."""

    def build(self, table: str, columns: list, values: list) -> SQLCommand:

        if (len(columns) != len(values)):
            raise ValueError(
                "Number of columns doesn't match number of values")

        command = SQLCommand(
            name='insert',
            description="Insert into {} table".format(table),
            cmd=sql.SQL("INSERT into {} ({}) values ({})").format(
                sql.Identifier(table),
                sql.SQL(', ').join(map(sql.Identifier, columns)),
                sql.SQL(', ').join(map(sql.Placeholder, values))),
            params=tuple((values,))
        )

        return command


# --------------------------------------------------------------------------- #
#                                 UPDATE                                      #
# --------------------------------------------------------------------------- #
class Update(QueryBuilder):
    """Generates SQL to support single value update commands."""

    def build(self, table: str, column: str, value: Union[str, float, int],
              where_key: str,
              where_value=Union[str, float, int]) -> SQLCommand:

        command = SQLCommand(
            name='update',
            description="Update {} table".format(table),
            cmd=sql.SQL("UPDATE {} SET {} = {} WHERE {} = {}").format(
                sql.Identifier(table),
                sql.Identifier(column),
                sql.Placeholder(),
                sql.Identifier(where_key),
                sql.Placeholder()),
            params=tuple((value, where_value,))
        )

        return command

# --------------------------------------------------------------------------- #
#                                 UPDATE                                      #
# --------------------------------------------------------------------------- #


class Delete(QueryBuilder):
    """Generates SQL to support basic delete commands."""

    def build(self, table: str, where_key: str,
              where_value=Union[str, float, int]) -> SQLCommand:

        command = SQLCommand(
            name='delete',
            description="Delete from {} table".format(table),
            cmd=sql.SQL("DELETE FROM {} WHERE {} = {}").format(
                sql.Identifier(table),
                sql.Identifier(where_key),
                sql.Placeholder()),
            params=tuple((where_value,))
        )

        return command
