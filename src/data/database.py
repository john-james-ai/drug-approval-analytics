#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#==============================================================================#
# Project  : Predict-FDA                                                       #
# Version  : 0.1.0                                                             #
# File     : \aact_db.py                                                       #
# Language : Python 3.9.5                                                      #
# -----------------------------------------------------------------------------#
# Author   : John James                                                        #
# Company  : nov8.ai                                                           #
# Email    : john.james@nov8.ai                                                #
# URL      : https://github.com/john-james-sf/predict-fda                      #
# -----------------------------------------------------------------------------#
# Created  : Monday, June 21st 2021, 3:17:33 pm                                #
# Modified : Thursday, July 1st 2021, 11:01:57 pm                              #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#
import os
import shutil
from datetime import datetime

from subprocess import Popen, PIPE, SubprocessError
from psycopg2 import connect, pool, sql, DatabaseError
import pandas as pd
import numpy as np
import psycopg2

from configs.config import Config
from src.logging import Logger, exception_handler
# -----------------------------------------------------------------------------#
#                           DATABASE CONNECTION                                #
# -----------------------------------------------------------------------------# 
class DBCon:

    __connection_pool = None

    @staticmethod
    def initialise(database):
        print(database)
        print("Initializing database. Defaults to the AACT database")
        credentials = Config.get(section=database + '_credentials')
        DBCon.__connection_pool = pool.SimpleConnectionPool(2, 10, **credentials)

    @staticmethod
    def get_connection():        
        con =  DBCon.__connection_pool.getconn()
        print("Obtained connection to AACT database.")
        return con
        

    @staticmethod
    def return_connection(connection):        
        DBCon.__connection_pool.putconn(connection)
        print("Returned connection to AACT database.")

    @staticmethod
    def close_all_connections():        
        DBCon.__connection_pool.closeall()
        print("Closed all connections to AACT database.")


# -----------------------------------------------------------------------------#
#                     DATA ACCESS OBJECT BASE CLASS                            #
# -----------------------------------------------------------------------------#
class DBDao:
    def __init__(self, database='aact', dbcon=DBCon):
        self._logger = Logger()
        self._dbcon = dbcon
        self._dbcon.initialise(database=database)
        self._config = Config()
        print("Acquiring connection")        
        self.schema_name = self._config.get(database)['schema']
        self.schema = None
        self.table_names = None
        self.connect()

    def __del__(self):
        print("Closing db connection")        
        self.connection.close()

    def connect(self):
        self.connection = self._dbcon.get_connection()        

        
    def read_table(self, table, idx=None, coerce_float=True, parse_dates=None):
        query = "SELECT * FROM {schema}.{table};".format(schema=self.schema_name,table=table)
        df = pd.read_sql_query(sql=query, con=self.connection, index_col=idx, 
                                coerce_float=coerce_float, parse_dates=parse_dates)
        return df    


# -----------------------------------------------------------------------------#
#                       DATABASE ADMINISTRATION                                #
# -----------------------------------------------------------------------------#
class DBAdmin:
    def __init__(self, database='aact'):        
        DBCon.initialise(database=database)
        self._config = Config()
        self._schema_name = self._config.get(database)['schema']
        self._schema = None
        self._table_names = None
        self._connect()

    def __del__(self):
        print("Closing db connection")        
        self._connection.close()

    def connect(self):
        self._connection = DBCon.get_connection()        

    def get_schema(self):
        if self._schema is None:
            query = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE "
            query += "table_schema = '{schema}' ORDER BY table_name;".format(schema=self._schema_name)            
            self._schema = pd.read_sql_query(query, con=self._connection)

    @exception_handler
    def create_table(self, command):        
        """Creates a PostgreSQL Table"""
        cur = self._connection.cursor()
        cur.execute(command)
        cur.close()
        cur.commit()
        DBCon.return_connection(self._connection)            

    def get_table_names(self):
        if self._table_names is None:
            query = "SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE "
            query += "table_schema = '{schema}'".format(schema=self._schema_name)
            self._table_names = list(pd.read_sql_query(query, con=self._connection)["table_name"].values)

            # Remove internal metadata. This is some Rails or Oracle related database object.
            self._table_names.remove("ar_internal_metadata")
        return self._table_names

    def get_columns(self, table, dtype=None):
        self._get_schema()
        columns = self._schema.loc[(self._schema["table_name"]==table)]
        if dtype == "date":
            columns = columns.loc[(columns["column_name"].str.endswith("date")) | 
                                    (columns["column_name"].str.endswith("at"))]
        columns = list(np.array(columns["column_name"].values).flatten())
        if len(columns) == 0:
            columns = None
        return columns    

        

