#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# =========================================================================== #
# Project  : Drug Approval Analytics                                          #
# Version  : 0.1.0                                                            #
# File     : \src\platform\repository.py                                      #
# Language : Python 3.9.5                                                     #
# --------------------------------------------------------------------------  #
# Author   : John James                                                       #
# Company  : nov8.ai                                                          #
# Email    : john.james@nov8.ai                                               #
# URL      : https://github.com/john-james-sf/drug-approval-analytics         #
# --------------------------------------------------------------------------  #
# Created  : Wednesday, July 21st 2021, 1:25:36 pm                            #
# Modified : Saturday, July 24th 2021, 3:56:37 am                             #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
"""Repository for pipeline events

This module defines the repository for pipeline events and consists of:

    - RepoSetup: Configures the Repository database.
    - RepoDBAdmin: Repository database administration
    - RepoDBAccess: Repository database access object.
    - Repository: Repository class interface used by the Pipeline


"""
from abc import ABC, abstractmethod
from uuid import UUID
import pandas as pd

# --------------------------------------------------------------------------- #


class RepositorySetup():
    """ Class the creates the Repository database.

    The repository is a one-table database, the schema for which is defined
    in the database configuration file.

    Arguments:
        configfilepath (str): Location of the Repository configuration file.

    """

    configfile = '../data/metadata/repository.csv'

    def __init__(self) -> None:
        self._configfile = RepoSetup.configfile

    def execute(self) -> None:
        """Creates the DataElements table."""

        df = pd.read_csv(self._configfile)
        config = df.to_dict('index')
        return config


class Repositories:
    """Repository for Pipeline Event objects.

    Arguments:
        connection: PipelineDatabaseAccessObject.connection

    """

    table = 'events'

    def __init__(self, connection):
        super(Events, self).__init__(connection)

    def add(self, event: Event) -> None:
        self._connection.add(Events.table, event)

    def get(self, name: str) -> Event:
        return self._connection.get(Events.table, name)

    def get_all(self) -> list:
        return self._connection.get_all(Events.table)

    def update(self, event: Event) -> None:
        self._connection.update(Events.table, event)

    def delete(self, name: str) -> None:
        self._connection.delete(Events.table, name)

    def delete_all(self) -> None:
        self._connection.delete_all(Events.table)


# --------------------------------------------------------------------------- #


class RepoDBAdmin:
    """Database administration for the repository database."""

    dbname = "repository"

    def __init__(self, name: str, engine: DBCon) -> None:
        self._name = RepoDBAdmin.dbname if name is None else name
        self._engine = engine

    @exception_handler()
    def create_database(self) -> str:
        """Creates the repository database and returns the database name."""
        return self._dba.create_database(self._name)

    @exception_handler()
    def drop_database(self) -> str:
        """Creates the repository database and returns the database name."""
        self._dba.drop_database(self._name)

    @exception_handler()
    def create_table(self, table: str, sql_query: str) -> None:
        conn = self._engine.initialize(
            repository_config).get_connection()
        cursor = conn.cursor()
        cursor.execute(sql_query)
        cursor.close()
        conn.commit()
        conn.close()
# --------------------------------------------------------------------------- #


class RepoDBAccess(ABC):
    """Base database access object for the pipeline repository Database."""

    dbname = "repository"

    def __init__(self, engine: DBCon, query_builder) -> None:
        self._engine = engine
        self._dbname = RepoDBAccess.dbname
        self._query_builder = query_builder
        self._connection = None

    @exception_handler()
    def get_connection(self) -> None:
        """Establishes a connection to the database."""

        # Connections will be managed by the client to enforce transaction
        # over the Pipeline event transaction.
        self._engine.initialize(repository_config)
        self._connection = self._engine.get_connection()
        self._cursor = self._connection.cursor()

    def list_events(self, stage: str = None) -> pd.DataFrame:
        if stage:

    def get_pipeline(self, name: str) -> Pipeline:
        self._q

    def get_event(self, name: str) -> dict:
        """Returns all components associated with the named pipeline event.

        An event is a hierarchy of objects, including the associated
        pipeline, the pipeline steps,  and the input and output
        elements of each pipeline step. Hence, this method returns
        a dictionary containing each component with names as keys.

        Arguments
            name (str): The unique name of the event.
        """
        return pd.read_sql_query(sql=sql_query, con=self._connection)

    def delete(self, table: str, sql_query: str,
               parameters: tuple = ()) -> None:
        self._cursor.execute(sql_query, parameters)
# --------------------------------------------------------------------------- #


class Event:
    """Element repository database.

    Arguments:
        dao (RepoDBAcess): Database access object
    """

    def __init__(sel, dao: RepoDBAccess, query_builder: QueryBuilder) -> None:
        self._dao = dao

    def get(self, event_name: str) -> Element:
