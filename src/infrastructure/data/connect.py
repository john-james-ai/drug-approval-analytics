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
# Modified : Tuesday, August 17th 2021, 4:59:14 am                            #
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
from src.application.config import DBCredentials
# --------------------------------------------------------------------------- #
logger = logging.getLogger(__name__)
# --------------------------------------------------------------------------- #


class Response:
    """Class representing responses from database commands."""

    def __init__(self, cursor=None, execute=None, fetchone=None,
                 fetchall=None, description=None, rowcount: int = 0):
        self.cursor = cursor
        self.execute = execute
        self.fetchall = fetchall
        self.fetchone = fetchone
        self.rowcount = rowcount
        self.description = description


# --------------------------------------------------------------------------- #
class Command:
    """This class is used to execute commands against the database."""

    def __init__(self):
        self._cursor = None
        self._prior_command = None

    @exception_handler()
    def execute_next(self, cursor) -> Response:
        response_fetchone = cursor.fetchone()
        response_description = cursor.description
        response = Response(cursor=cursor,
                            fetchone=response_fetchone,
                            description=response_description)

        return response

    @exception_handler()
    def execute_one(self, sequel: Sequel, connection) -> Response:
        cursor = connection.cursor()
        response_execute = cursor.execute(sequel.cmd, sequel.params)
        response_description = cursor.description
        response_rowcount = cursor.rowcount
        response_fetchone = cursor.fetchone()

        response = Response(cursor=cursor,
                            execute=response_execute,
                            fetchone=response_fetchone,
                            description=response_description,
                            rowcount=response_rowcount)

        logger.info(sequel.description)
        return response

    @exception_handler()
    def execute(self, sequel: Sequel, connection) -> Response:
        cursor = connection.cursor()
        response_execute = cursor.execute(sequel.cmd, sequel.params)
        response_description = cursor.description
        response_rowcount = cursor.rowcount

        try:
            response_fetchall = cursor.fetchall()
        except psycopg2.ProgrammingError:
            response_fetchall = None

        response = Response(cursor=cursor,
                            execute=response_execute,
                            fetchall=response_fetchall,
                            description=response_description,
                            rowcount=response_rowcount)
        cursor.close()
        logger.info(sequel.description)
        return response

    @exception_handler()
    def execute_ddl(self, sequel: Sequel, connection) -> None:
        """Processes SQL DDL commands from file."""

        with connection.cursor() as cursor:
            cursor.execute(open(sequel.params, "r").read())
        cursor.close()
        logger.info(sequel.description)


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
    def return_connection(connection) -> None:
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


# --------------------------------------------------------------------------- #
#                          CONNECTION POOL CLASS                              #
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

    def __init__(self, credentials: DBCredentials) -> None:
        self._connection_pool = PGConnectionPool.initialize(credentials)
        self._connection = None
        self._autocommit = True

    def __del__(self):
        self._connection_pool.return_connection(self._connection)

    def begin_transaction(self):
        self._autocommit = False
        self.open_connection()

    def end_transaction(self):
        self.commit()

    def open_connection(self) -> None:
        if self._connection is None or self._connection.closed:
            self._connection = self._connection_pool.get_connection()
        self._connection.set_session(autocommit=self._autocommit)

    def close_connection(self):
        self._connection.return_connection(self._connection)

    def commit(self):
        self._connection.commit()

    def rollback(self):
        self._connection.rollback()

    @property
    def autocommit(self) -> bool:
        return self._autocommit

    @autocommit.setter
    def autocommit(self, autocommit) -> None:
        self._autocommit = autocommit
        if not self.closed:
            self._connection.set_session(autocommit=self._autocommit)

    @property
    def cursor(self):
        return self._connection.cursor

    @property
    def closed(self):
        if self._connection:
            if self._connection.closed:
                return True
            else:
                return False
        else:
            return True
