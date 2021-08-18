#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# =========================================================================== #
# Project  : Drug Approval Analytics                                          #
# Version  : 0.1.0                                                            #
# File     : \src\infrastructure\database\connect.py                          #
# Language : Python 3.9.5                                                     #
# --------------------------------------------------------------------------  #
# Author   : John James                                                       #
# Company  : nov8.ai                                                          #
# Email    : john.james@nov8.ai                                               #
# URL      : https://github.com/john-james-sf/drug-approval-analytics         #
# --------------------------------------------------------------------------  #
# Created  : Tuesday, August 3rd 2021, 4:47:23 am                             #
# Modified : Wednesday, August 18th 2021, 10:43:53 am                         #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
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
# Modified : Monday, August 16th 2021, 8:58:54 am                             #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
"""Core internal Base, Connection, and ConnectionPool classes."""
from abc import ABC, abstractmethod
import logging

import psycopg2
from psycopg2 import pool
from sqlalchemy import create_engine

from ...utils.logger import exception_handler
from .sequel import Sequel
from src.infrastructure.data.config import DBCredentials
# --------------------------------------------------------------------------- #
logger = logging.getLogger(__name__)
# --------------------------------------------------------------------------- #


# --------------------------------------------------------------------------- #
#                       CONNECTION POOL BASE CLASS                            #
# --------------------------------------------------------------------------- #


class AbstractConnectionPool(ABC):
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
    def close(connection) -> None:
        """Returns a connection to the connection pool if implemented."""
        pass

    @staticmethod
    @abstractmethod
    def close_all_connections() -> None:
        """Closes all open connections."""
        pass


# --------------------------------------------------------------------------- #
#                    POSTGRES CONNECTION POOL CLASS                           #
# --------------------------------------------------------------------------- #
class PGConnectionPool(AbstractConnectionPool):
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
    def close(connection) -> None:
        name = connection.info.dsn_parameters['dbname']
        logger.info(
            "Returning connection to {} connection pool.".format(name))
        PGConnectionPool.__connection_pool.putconn(connection)

    @staticmethod
    @exception_handler()
    def close_all_connections() -> None:
        PGConnectionPool.__connection_pool.closeall()


# --------------------------------------------------------------------------- #
#                     SQLALCHEMY CONNECTOR POOL CLASS                         #
# --------------------------------------------------------------------------- #
class SAConnectionPool(AbstractConnectionPool):
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
    def close(connection) -> None:
        connection.close()
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


# --------------------------------------------------------------------------- #
#                            CONNECTION CLASS                                 #
# --------------------------------------------------------------------------- #


class Connection:
    """Encapsulates a connection pool and the behavior of its connections.

    Arguments:
        credentials (DBCredentials): Credentials including the database name.

    Dependencies:
        PGConnectionPool: Pool of connections to the database.

    Raises:
        DatabaseError if database does not exist.
    """

    def __init__(self, credentials: DBCredentials, autocommit: bool = True, postgres: bool = True) -> None:
        self._credentials = credentials
        self._autocommit = autocommit
        self._postgres = postgres

        if postgres:
            PGConnectionPool.initialize(credentials)
            self._connection = self._get_connection(autocommit)
        else:
            SAConnectionPool.initialize(credentials)
            self._connection = self._get_connection()

    def __del__(self):
        self.close()

    def __enter__(self):
        self.begin_transaction()
        return self

    def __exit__(self, type, value, traceback):
        self.commit()

    def _get_connection(self, autocommit=True):
        if self._postgres:
            connection = PGConnectionPool.get_connection()
            connection.set_session(autocommit=autocommit)

        else:
            connection = SAConnectionPool.get_connection()
        return connection

    def begin_transaction(self):
        self.close()
        self._connection = self._get_connection(autocommit=False)

    def commit(self):
        self._connection.commit()

    def close(self):
        if self._connection is not None:
            if self._postgres:
                PGConnectionPool.close(self._connection)
            else:
                SAConnectionPool.close(self._connection)

    def rollback(self):
        self._connection.rollback()

    @property
    def dbname(self):
        return self._credentials.dbname

    @property
    def user(self):
        return self._credentials.user

    @property
    def cursor(self):
        if self._postgres:
            return self._connection.cursor
        else:
            return self._connection.connect()
