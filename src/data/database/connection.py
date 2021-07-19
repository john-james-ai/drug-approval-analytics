#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#==============================================================================#
# Project  : Drug Approval Analytics                                           #
# Version  : 0.1.0                                                             #
# File     : \src\data\database\connection.py                                  #
# Language : Python 3.9.5                                                      #
# -----------------------------------------------------------------------------#
# Author   : John James                                                        #
# Company  : nov8.ai                                                           #
# Email    : john.james@nov8.ai                                                #
# URL      : https://github.com/john-james-sf/drug-approval-analytics          #
# -----------------------------------------------------------------------------#
# Created  : Monday, July 19th 2021, 1:39:12 pm                                #
# Modified : Monday, July 19th 2021, 1:48:08 pm                                #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#
import logging

import psycopg2
from psycopg2 import connect, pool, sql
# -----------------------------------------------------------------------------#
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

      
# -----------------------------------------------------------------------------#
#                          DATABASE CONNECTION                                 #
# -----------------------------------------------------------------------------# 
class DBCon:

    __connection_pool = None

    @staticmethod
    def initialise(credentials):         
        DBCon.__connection_pool = pool.SimpleConnectionPool(2, 10, **credentials)
        logger.info("Initialized connection pool for {} database.".format(credentials['dbname']))
    
    @staticmethod
    def get_connection():        
        con =  DBCon.__connection_pool.getconn()
        dbname = con.info.dsn_parameters['dbname']
        logger.info("Getting connection from {} connection pool.".format(dbname))
        return con        

    @staticmethod
    def return_connection(connection):        
        DBCon.__connection_pool.putconn(connection)
        dbname = connection.info.dsn_parameters['dbname']
        logger.info("Returning connection to {} connection pool.".format(dbname))

    @staticmethod
    def close_all_connections():        
        DBCon.__connection_pool.closeall()