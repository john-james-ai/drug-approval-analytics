#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# =========================================================================== #
# Project  : Drug Approval Analytics                                          #
# Version  : 0.1.0                                                            #
# File     : \src\platform\data\admin.py                                      #
# Language : Python 3.9.5                                                     #
# --------------------------------------------------------------------------  #
# Author   : John James                                                       #
# Company  : nov8.ai                                                          #
# Email    : john.james@nov8.ai                                               #
# URL      : https://github.com/john-james-sf/drug-approval-analytics         #
# --------------------------------------------------------------------------  #
# Created  : Tuesday, August 3rd 2021, 12:27:05 pm                            #
# Modified : Friday, August 13th 2021, 7:25:00 pm                             #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
"""Database setup module."""
from abc import ABC, abstractmethod
import logging

import pandas as pd
from psycopg2 import sql

from .connect import PGConnectionFactory, SAConnectionFactory
from ..utils.logger import exception_handler
# --------------------------------------------------------------------------- #
logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
#                               BUILDER                                       #
# --------------------------------------------------------------------------- #
class Builder(ABC):
    """Abstract base class for general database and table builder classes."""

    @abstractmethod
    def build(self, connection, *args, **kwargs) -> None:
        pass


# --------------------------------------------------------------------------- #
class CopyDatabase:
    """Creates a copy of a database on the same server."""

    @exception_handler()
    def build(self, connection, sourcedb: str, targetdb: str) -> None:
        """Copies a source database to a target database on same server.

        Arguments
            connection (psycopg2.connection): Connect to the database
            sourcedb (str): The name of the source database.
            targetdb (str): The name of the target database.

        """

        command = sql.SQL("CREATE DATABASE {} WITH TEMPLATE {};").format(
            sql.Identifier(sourcedb),
            sql.Identifier(targetdb)
        )

        cursor = connection.cursor()
        cursor.execute(command)
        cursor.close()

        logger.info("Created {} database as copy of {}".format(
            targetdb, sourcedb))


# --------------------------------------------------------------------------- #
class CreateTableFromDataFrame:
    """Creates a table from a pandas DataFrame object. """

    @exception_handler()
    def build(self, connection, tablename: str, df: pd.DataFrame,
              **kwargs) -> None:
        """Creates a database table from a dataframe using pandas facility.

        Arguments:
            connection (sqlalchemy.engine.Connection): Database connection
            tablename (str): Name of table
            df (pd.DataFrame): Pandas DataFrame containing the data
            kwargs (dict): Keyword parameters passed to pandas
                DataFrame.to_sql.

        """

        df.to_sql(name=tablename, con=connection, **kwargs)


def get_connections(credentials: DBCredentials) -> tuple:
    """Returns connections for the designated connector object.

    Argument
        credentials dict: Dictionary containing database credentials.
    """
    connector = PGConnectionFactory()
    connector.initialize(credentials)
    pg_connection = connector.get_connection()

    connector = SAConnectionFactory()
    connector.initialize(credentials)
    sa_connection = connector.get_connection()

    return (pg_connection, sa_connection)


def setup(credentials: DBCredentials) -> None:
    """Sets up the database and tables for the project.

    Argument
        credentials (dict): Credentials for the postgres database.

    """
    pg_connection, sa_connection = get_connections(credentials)

    # create(pg_connection)
    # create_clinical_trials_tables(connection)
    # create_drugs_tables(connection)
    # create_biologics_tables(connection)
    # create_labels_tables(connection)
    # create_chembl_tables(connection)

    # connector.return_connection(connection)
