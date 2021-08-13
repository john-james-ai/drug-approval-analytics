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
# Modified : Tuesday, August 10th 2021, 8:14:16 pm                            #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
"""Core internal Base, Connection, and ConnectionFactory classes."""
from abc import ABC, abstractmethod
import logging

from psycopg2 import pool
from sqlalchemy import create_engine

from ...utils.logger import exception_handler
from ..config import DBCredentials
# --------------------------------------------------------------------------- #
logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
#                       CONNECTION FACTORY BASE CLASS                         #
# --------------------------------------------------------------------------- #

class ConnectionFactory(ABC):
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
class PGConnectionFactory(ConnectionFactory):
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

        PGConnectionFactory.__connection_pool = pool.SimpleConnectionPool(
            mincon, maxcon, **credentials)

        logger.info("Initialized connection pool for {} database.".format(
            credentials['dbname']))

    @staticmethod
    @exception_handler()
    def get_connection():
        con = PGConnectionFactory.__connection_pool.getconn()
        name = con.info.dsn_parameters['dbname']
        logger.info(
            "Getting connection from {} connection pool.".format(name))
        return con

    @staticmethod
    @exception_handler()
    def return_connection(connection) -> None:
        PGConnectionFactory.__connection_pool.putconn(connection)
        name = connection.info.dsn_parameters['dbname']
        logger.info(
            "Returning connection to {} connection pool.".format(name))

    @staticmethod
    @exception_handler()
    def close_all_connections() -> None:
        PGConnectionFactory.__connection_pool.closeall()


# --------------------------------------------------------------------------- #
#                      SQLALCHEMY CONNECTOR CLASS                             #
# --------------------------------------------------------------------------- #
class SAConnectionFactory(ConnectionFactory):
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
        SAConnectionFactory.initialized = False
        USER = credentials['user']
        PWD = credentials['password']
        HOST = credentials['host']
        PORT = credentials['port']
        DBNAME = credentials['dbname']

        DATABASE_URI = f'postgresql://{USER}:{PWD}@{HOST}:{PORT}/'

        SAConnectionFactory.__connection_pool = \
            create_engine(f'{DATABASE_URI}{DBNAME}',
                          pool_size=pool_size, max_overflow=max_overflow)

        logger.info("Initialized {} connection pool for {} database.".format(
            SAConnectionFactory.__class__.__name__,
            credentials['dbname']))

    @staticmethod
    @exception_handler()
    def get_connection():
        """Returns a connection engine object."""
        connection = SAConnectionFactory.__connection_pool
        logger.info(
            "Obtained SQLAlchemy connection from connection pool.")
        return connection

    @staticmethod
    @exception_handler()
    def return_connection(connection) -> None:
        connection.dispose()
        logger.info(
            "Returned connection to {} connection pool.".format(
                SAConnectionFactory.__class__.__name__,))

    @staticmethod
    @exception_handler()
    def close_all_connections() -> None:
        SAConnectionFactory.__connection_pool.dispose()
        logger.info(
            "Closed all {} connections.".format(
                SAConnectionFactory.__class__.__name__,))
