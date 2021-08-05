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
# Modified : Thursday, August 5th 2021, 12:18:42 am                           #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
"""SQL Generator: Generates SQL strings compatible with psycopg2.

"""
from abc import ABC, abstractmethod
from psycopg2 import sql
from dataclasses import dataclass, field

# --------------------------------------------------------------------------- #
#                              SQL COMMAND                                    #
# --------------------------------------------------------------------------- #


@dataclass
class Sequel:
    """Class that encapsulates a sql command, its name and parameters."""
    name: str
    cmd: sql.SQL
    description: str = field(default=None)
    params: tuple = field(default=())
# --------------------------------------------------------------------------- #
#                                SequelGen                                    #
# --------------------------------------------------------------------------- #


class SequelGen(ABC):
    """Defines the API for SQL Generator classes."""

    @abstractmethod
    def create(self, name: str, *args, **kwargs) -> sql.SQL:
        pass

    @abstractmethod
    def drop(self, name: str) -> sql.SQL:
        pass

    @abstractmethod
    def exists(self, name: str) -> sql.SQL:
        pass


# --------------------------------------------------------------------------- #
#                             DATABASE SEQUEL                                 #
# --------------------------------------------------------------------------- #
class DatabaseSequel(SequelGen):

    def create(self, name: str) -> sql.SQL:
        command = Sequel(
            name="create_database",
            description="Created {} database".format(name),
            cmd=sql.SQL("CREATE DATABASE {} ;").format(
                sql.Identifier(name))
        )

        return command

    def drop(self, name: str) -> sql.SQL:
        command = Sequel(
            name="drop database",
            description="Dropped {} database if it exists.".format(name),
            cmd=sql.SQL("DROP DATABASE IF EXISTS {} ;").format(
                sql.Identifier(name))
        )

        return command

    def terminate(self, name: str) -> sql.SQL:
        command = Sequel(
            name="terminate_database_processes",
            description="Terminated processes on {} database if it exists."
            .format(name),
            cmd=sql.SQL("""SELECT pg_terminate_backend(pg_stat_activity.pid)
                        FROM pg_stat_activity
                        WHERE pg_stat_activity.datname = {};""").format(
                sql.Placeholder()
            )
        )

        return command

    def exists(self, name: str) -> sql.SQL:
        command = Sequel(
            name="database exists",
            description="Checked existence of {} database.".format(name),
            cmd=sql.SQL("""SELECT EXISTS(
                    SELECT datname FROM pg_catalog.pg_database
                    WHERE lower(datname) = lower(%s));"""),
            params=tuple((name,))
        )

        return command


# --------------------------------------------------------------------------- #
#                               USER SEQUEL                                   #
# --------------------------------------------------------------------------- #
class UserSequel(SequelGen):

    def create(self, name: str) -> sql.SQL:
        command = Sequel(
            name="create_user",
            description="Created user {}".format(name),
            cmd=sql.SQL("CREATE USER {} CREATEDB;").format(
                sql.Identifier(name))
        )

        return command

    def drop(self, name: str) -> sql.SQL:
        command = Sequel(
            name="drop_user",
            description="Dropped user {}".format(name),
            cmd=sql.SQL("DROP USER IF EXISTS {};").format(
                sql.Identifier(name))
        )

        return command

    def exists(self, name: str) -> sql.SQL:
        command = Sequel(
            name="user_exists",
            description="Checked existence of user {}".format(name),
            cmd=sql.SQL("SELECT 1 FROM pg_roles WHERE rolname ={};").format(
                sql.Placeholder()),
            params=tuple((name,))
        )

        return command

    def grant(self, name: str, dbname: str) -> sql.SQL:
        command = Sequel(
            name="grant",
            description="Granted privileges on database {} to {}"
            .format(dbname, name),
            cmd=sql.SQL("GRANT ALL PRIVILEGES ON DATABASE {} TO {} ;")
            .format(
                sql.Identifier(dbname),
                sql.Identifier(name))
        )

        return command

    def revoke(self, name: str, dbname: str) -> sql.SQL:
        command = Sequel(
            name="revoke",
            description="Revoked privileges on database {} to {}"
            .format(dbname, name),
            cmd=sql.SQL("REVOKE ALL PRIVILEGES ON DATABASE {} TO {} ;")
            .format(
                sql.Identifier(dbname),
                sql.Identifier(name))
        )

        return command
# --------------------------------------------------------------------------- #
#                              TABLES SEQUEL                                  #
# --------------------------------------------------------------------------- #


class TableSequel(SequelGen):

    def create(self, name: str) -> sql.SQL:
        msg = "Tables are created with data using pandas to_sql facility. "
        msg += "No query required."
        raise NotImplementedError(msg)

    def drop(self, name: str, schema: str) -> sql.SQL:
        name = ".".join(schema, name)
        command = Sequel(
            name="drop_table",
            description="Dropped table {}".format(name),
            cmd=sql.SQL("DROP TABLE IF EXISTS {} CASCADE;").format(
                sql.Identifier(name))
        )

        return command

    def exists(self, name: str, schema: str) -> sql.SQL:
        subquery = sql.SQL("""SELECT 1 FROM information_schema.tables
                                WHERE table_schema = {}
                                AND table_name = {}""").format(
            sql.Identifier(schema),
            sql.Identifier(name)
        )
        command = Sequel(
            name="table_exists",
            description="Checked existence of table {}".format(name),
            cmd=sql.SQL("SELECT EXISTS({});").format(subquery)
        )

        return command

    def column_exists(self, name: str, schema: str, tablename: str) -> sql.SQL:
        subquery = sql.SQL("""SELECT 1 FROM information_schema.columns
                                WHERE table_schema = {}
                                AND table_name = {}
                                AND column_name = {}""").format(
            sql.Identifier(schema),
            sql.Identifier(schema),
            sql.Identifier(name)
        )
        command = Sequel(
            name="column_exists",
            description="Checked existence of column {} in {} table".format(
                name, tablename),
            cmd=sql.SQL("SELECT EXISTS({});").format(subquery)
        )

        return command

    def add_column(self, name: str, schema: str, column: str, datatype: str,
                   constraint: str = None) -> sql.SQL:

        if constraint:
            sequel = sql.SQL("""ALTER TABLE {}
                        ADD COLUMN IF NOT EXISTS {} {} {};""").format(
                sql.Identifier(name),
                sql.Identifier(column),
                sql.Identifier(datatype),
                sql.Identifier(constraint)
            )
        else:
            sequel = sql.SQL("""ALTER TABLE {}
                        ADD COLUMN IF NOT EXISTS {} {};""").format(
                sql.Identifier(name),
                sql.Identifier(column),
                sql.Identifier(datatype)
            )

        command = Sequel(
            name="add_column",
            description="Added column {} to  table {}".format(
                column, name),
            cmd=sequel
        )

        return command

    def drop_column(self, name: str, schema: str, column: str) -> sql.SQL:
        command = Sequel(
            name="drop_column",
            description="Dropped column {}".format(name),
            cmd=sql.SQL("""ALTER TABLE {}
                        DROP COLUMN IF EXISTS {} CASCADE;""").format(
                sql.Identifier(name),
                sql.Identifier(name)
            )
        )

        return command


# --------------------------------------------------------------------------- #
#                              SCHEMA SEQUEL                                  #
# --------------------------------------------------------------------------- #


class SchemaSequel(SequelGen):

    def create(self, name: str) -> sql.SQL:
        command = Sequel(
            name="create_schema",
            description="Created schema {}".format(name),
            cmd=sql.SQL("CREATE SCHEMA {} ;").format(
                sql.Identifier(name))
        )

        return command

    def drop(self, name: str) -> sql.SQL:
        command = Sequel(
            name="drop_schema",
            description="Dropped schema {}".format(name),
            cmd=sql.SQL("DROP SCHEMA {} CASCADE;").format(
                sql.Identifier(name))
        )

        return command

    def exists(self, name: str) -> sql.SQL:
        subquery = sql.SQL("""SELECT 1 FROM information_schema.schemata
                                WHERE schema_name = {}""").format(
            sql.Identifier(name)
        )
        command = Sequel(
            name="schema_exists",
            description="Checked existence of schema {}".format(name),
            cmd=sql.SQL("SELECT EXISTS({});").format(subquery)
        )

        return command
