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
# Modified : Sunday, August 15th 2021, 7:52:44 am                             #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
"""Core internal Base, Connection, and ConnectionPool classes."""
from abc import ABC, abstractmethod
import logging

from psycopg2 import pool
from sqlalchemy import create_engine

from ...utils.logger import exception_handler
from ..config import DBCredentials
# --------------------------------------------------------------------------- #
logger = logging.getLogger(__name__)
# --------------------------------------------------------------------------- #
#                          CONNECTION BASE CLASS                              #
# --------------------------------------------------------------------------- #


class Connection:
    """Database Connection Class."""

    def __init__(connection_pool: ConnectionPool,
                 sequel: Sequel,
                 command: Command) -> None:
        self._connection_pool = connection_pool
        self._sequel = sequel
        self._command = command
        self._connection = None

    def __del__(self):
        self._connection.close()
        self._connection_pool.return_connection(self._connection)
        self._connection_pool.close_all_connections()

    def begin_transaction(self, isolation_level: str = None):
        self._connection = self._connection_pool.get_connection()
        self._connection.set_session(isolation_level)
        sequel = self._sequel.begin()
        self._command.execute(sequel, self._connection)

    def end_transaction(self):
        self._connection.commit()

    def open(self):
        if not self._connection:
            self._connection = self._connection_pool.get_connection()

    def close(self):
        self._connection.close()

    def commit(self):
        self._connection.commit()

    def rollback(self):
        self._connection.rollback()

    def change_database(self, credentials: DBCredentials) -> None:
        self._connection_pool.return_connection(self._connection)
        self._connection_pool.initialize(credentials)
        self._connection = self._connection_pool.get_connection()


# --------------------------------------------------------------------------- #
#                           CONNECTION FACTORY                                #
# --------------------------------------------------------------------------- #
class ConnectionFactory:
    """Creates connection objects. """

    def __init__(self, credentials: DBCredentials,
                 connection_pool: ConnectionPool,
                 sequel: Sequel
                 command: Command):
        self._connection_pool = connection_pool.intialize(credentials)
        self._sequel = sequel
        self._command = command

    def build_connection(self):
        connection = Connection(self._connection_pool,
                                self._sequel, self._command)
        return connection
# --------------------------------------------------------------------------- #
#                       CONNECTION POOL BASE CLASS                            #
# --------------------------------------------------------------------------- #


class ConnectionPool(ABC):
    """Interface for database connection factories."""

    __connection_pool = None

    @staticmethod
    @abstractmethod
    def initialize(credentials: DBCredentials, *args, **kwargs) -> None:
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
class PGConnectionPool(ConnectionPool):
    """Postgres database connection pool."""

    __connection_pool = None

    @staticmethod
    @exception_handler()
    def initialize(credentials: DBCredentials, mincon: int = 2,
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

        PGConnectionPool.__connection_pool = pool.SimpleConnectionPool(
            mincon, maxcon, **credentials)

        logger.info("Initialized connection pool for {} database.".format(
            credentials['dbname']))

    @staticmethod
    @exception_handler()
    def get_connection():
        con = PGConnectionPool.__connection_pool.getconn()
        name = con.info.dsn_parameters['dbname']
        logger.info(
            "Getting connection from {} connection pool.".format(name))
        return con

    @staticmethod
    @exception_handler()
    def return_connection(connection) -> None:
        PGConnectionPool.__connection_pool.putconn(connection)
        name = connection.info.dsn_parameters['dbname']
        logger.info(
            "Returning connection to {} connection pool.".format(name))

    @staticmethod
    @exception_handler()
    def close_all_connections() -> None:
        PGConnectionPool.__connection_pool.closeall()


# --------------------------------------------------------------------------- #
#                      SQLALCHEMY CONNECTOR CLASS                             #
# --------------------------------------------------------------------------- #
class SAConnectionPool(ConnectionPool):
    """SQLAlchemy database connection pool."""

    __connection_pool = None

    @staticmethod
    @exception_handler()
    def initialize(credentials: DBCredentials, pool_size: int = 5,
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
        SAConnectionPool.initialized = False
        USER = credentials['user']
        PWD = credentials['password']
        HOST = credentials['host']
        PORT = credentials['port']
        DBNAME = credentials['dbname']

        DATABASE_URI = f'postgresql://{USER}:{PWD}@{HOST}:{PORT}/'

        SAConnectionPool.__connection_pool = \
            create_engine(f'{DATABASE_URI}{DBNAME}',
                          pool_size=pool_size, max_overflow=max_overflow)

        logger.info("Initialized {} connection pool for {} database.".format(
            SAConnectionPool.__class__.__name__,
            credentials['dbname']))

    @staticmethod
    @exception_handler()
    def get_connection():
        """Returns a connection engine object."""
        connection = SAConnectionPool.__connection_pool
        logger.info(
            "Obtained SQLAlchemy connection from connection pool.")
        return connection

    @staticmethod
    @exception_handler()
    def return_connection(connection) -> None:
        connection.dispose()
        logger.info(
            "Returned connection to {} connection pool.".format(
                SAConnectionPool.__class__.__name__,))

    @staticmethod
    @exception_handler()
    def close_all_connections() -> None:
        SAConnectionPool.__connection_pool.dispose()
        logger.info(
            "Closed all {} connections.".format(
                SAConnectionPool.__class__.__name__,))
