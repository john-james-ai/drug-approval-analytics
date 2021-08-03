#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# =========================================================================== #
# Project  : Drug Approval Analytics                                          #
# Version  : 0.1.0                                                            #
# File     : \src\platform\database.py                                        #
# Language : Python 3.9.5                                                     #
# --------------------------------------------------------------------------  #
# Author   : John James                                                       #
# Company  : nov8.ai                                                          #
# Email    : john.james@nov8.ai                                               #
# URL      : https://github.com/john-james-sf/drug-approval-analytics         #
# --------------------------------------------------------------------------  #
# Created  : Friday, July 23rd 2021, 1:23:26 pm                               #
# Modified : Tuesday, August 3rd 2021, 5:36:19 am                             #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
"""Encapsulates database specific connection management.

Defines an abstraction for connecting to a database. Concrete subclasses
contain database specific connection management logic and will be
created as needed.

Classes:
    Database: Abstract base class defining the interface.
    PGConnector: Postgres connection implementation
    SAConnector: SQLAlchemy connection implementation
    MSDatabase: MySQL connection implementation (Future)

See Also:
    dbaccess: Database access objects.
    dbadmin: Database administration.

"""
import logging
import psycopg2.pool as pg_pool

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from .base import Connector
from ..utils.logger import exception_handler
# --------------------------------------------------------------------------- #
logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
#                    SQLALCHEMY DATABASE CONNECTION                           #
# --------------------------------------------------------------------------- #
class SAConnector(Connector):
    """SQLAlchemy database connection pool."""

    __connection_pool = None

    @staticmethod
    @exception_handler
    def initialize(credentials, pool_size=5, max_overflow=10):
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
    @exception_handler
    def get_connection():
        """Returns a session object."""
        # To be consistent with the postgrs connection abstraction, we will
        # operate on the session object and treat it as a connection.
        session = sessionmaker(SAConnector.__connection_pool)
        logger.info(
            "Obtained session from {} connection pool.".format(
                SAConnector.__class__.__name__,))
        return session()

    @staticmethod
    @exception_handler
    def return_connection(connection):
        connection.close()
        logger.info(
            "Returned connection to {} connection pool.".format(
                SAConnector.__class__.__name__,))

    @staticmethod
    @exception_handler
    def close_all_connections():
        SAConnector.__connection_pool.dispose()
        logger.info(
            "Closed all {} connections.".format(
                SAConnector.__class__.__name__,))
