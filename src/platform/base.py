#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# =========================================================================== #
# Project  : Drug Approval Analytics                                          #
# Version  : 0.1.0                                                            #
# File     : \src\platform\core\database.py                                   #
# Language : Python 3.9.5                                                     #
# --------------------------------------------------------------------------  #
# Author   : John James                                                       #
# Company  : nov8.ai                                                          #
# Email    : john.james@nov8.ai                                               #
# URL      : https://github.com/john-james-sf/drug-approval-analytics         #
# --------------------------------------------------------------------------  #
# Created  : Tuesday, August 3rd 2021, 4:47:23 am                             #
# Modified : Tuesday, August 3rd 2021, 7:35:48 pm                             #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
"""Defines base classes for database connections and access."""
from abc import ABC, abstractmethod
import logging

from psycopg2 import pool
from sqlalchemy import create_engine

from ..utils.logger import exception_handler
# --------------------------------------------------------------------------- #
logger = logging.getLogger(__name__)
# --------------------------------------------------------------------------- #
#                   DATABASE CONNECTOR BASE CLASS                             #
# --------------------------------------------------------------------------- #


class Connector(ABC):
    """Abstract base class for platform specific subclasses"""

    @staticmethod
    @abstractmethod
    def initialize(credentials: dict, *args, **kwargs) -> None:
        """Initializes connection pool"""
        pass

    @staticmethod
    @abstractmethod
    def get_connection():
        """Provides a connection to the database."""
        pass

    @staticmethod
    @abstractmethod
    def return_connection(connection) -> None:
        """Returns a connection to the connection pool if implemented."""
        pass

    @staticmethod
    @abstractmethod
    def close_all_connections() -> None:
        """Closes all open connections."""
        pass


# --------------------------------------------------------------------------- #
#                      POSTGRES CONNECTOR CLASS                               #
# --------------------------------------------------------------------------- #
class PGConnector(Connector):
    """Postgres database connection pool."""

    __connection_pool = None

    @staticmethod
    @exception_handler()
    def initialize(credentials: dict, mincon: int = 2,
                   maxcon: int = 10) -> None:
        """Initializes connection pool

        Arguments:
            credentials (dict): Dictionary containing dbname, host, user
                password, and port for the user.
            mincon (int, optional): Min connections in connection pool.
                Defaults to 2.
            maxcon (int, optional): Max connections in connection pool.
                Defaults to 10.
        """

        PGConnector.__connection_pool = pool.SimpleConnectionPool(
            mincon, maxcon, **credentials)
        logger.info("Initialized {} connection pool for {} database.".format(
            PGConnector.__class__.__name__,
            credentials['dbname']))

    @staticmethod
    @exception_handler()
    def get_connection():
        con = PGConnector.__connection_pool.getconn()
        name = con.info.dsn_parameters['dbname']
        logger.info(
            "Getting connection from {} connection pool.".format(name))
        return con

    @staticmethod
    @exception_handler()
    def return_connection(connection) -> None:
        PGConnector.__connection_pool.putconn(connection)
        name = connection.info.dsn_parameters['dbname']
        logger.info(
            "Returning connection to {} connection pool.".format(name))

    @staticmethod
    @exception_handler()
    def close_all_connections() -> None:
        PGConnector.__connection_pool.closeall()


# --------------------------------------------------------------------------- #
#                      SQLALCHEMY CONNECTOR CLASS                             #
# --------------------------------------------------------------------------- #
class SAConnector(Connector):
    """SQLAlchemy database connection pool."""

    __connection_pool = None

    @staticmethod
    @exception_handler()
    def initialize(credentials: dict, pool_size: int = 5,
                   max_overflow: int = 10) -> None:
        """Initializes connection pool

        Arguments:
            credentials (dict): Dictionary containing dbname, host, user
                password, and port for the user.
            pool_size (int, optional): Num connectionsi in pool.
                Defaults to 5.
            max_overflow (int, optional): Max number of pools above
                pool size. Defaults to 10.
        """

        USER = credentials['user']
        PWD = credentials['password']
        HOST = credentials['host']
        PORT = credentials['port']
        DBNAME = credentials['dbname']

        DATABASE_URI = f'postgresql://{USER}:{PWD}@{HOST}:{PORT}/'

        SAConnector.__connection_pool = \
            create_engine(f'{DATABASE_URI}{DBNAME}',
                          pool_size=pool_size, max_overflow=max_overflow)

        logger.info("Initialized {} connection pool for {} database.".format(
            SAConnector.__class__.__name__,
            credentials['dbname']))

    @staticmethod
    @exception_handler()
    def get_connection():
        """Returns a connection engine object."""
        connection = SAConnector.__connection_pool
        logger.info(
            "Obtained SQLAlchemy connection from connection pool.")
        return connection

    @staticmethod
    @exception_handler()
    def return_connection(connection) -> None:
        connection.close()
        logger.info(
            "Returned connection to {} connection pool.".format(
                SAConnector.__class__.__name__,))

    @staticmethod
    @exception_handler()
    def close_all_connections() -> None:
        SAConnector.__connection_pool.dispose()
        logger.info(
            "Closed all {} connections.".format(
                SAConnector.__class__.__name__,))
# --------------------------------------------------------------------------- #
#                      DATABASE ACCESS OBJECTS                                #
# --------------------------------------------------------------------------- #


class Access(ABC):
    """Abstract base class for data access objects."""

    @abstractmethod
    def create(self, connection, *args, **kwargs):
        pass

    @abstractmethod
    def read(self, connection, *args, **kwargs):
        pass

    @abstractmethod
    def update(self, connection, *args, **kwargs):
        pass

    @abstractmethod
    def delete(self, connection, name):
        pass
