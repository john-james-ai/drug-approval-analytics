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
# Modified : Saturday, June 26th 2021, 5:59:25 pm                              #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#
from psycopg2 import connect, pool, sql, DatabaseError
import pandas as pd

from configs.config import get_config
from .sqlgen import Queryator

class AACT:

    __connection_pool = None

    @staticmethod
    def initialise():
        print("Initializing AACT database.")
        credentials = get_config(section='aactdb')
        AACT.__connection_pool = pool.SimpleConnectionPool(1, 10, **credentials)

    @staticmethod
    def get_connection():        
        con =  AACT.__connection_pool.getconn()
        print("Obtained connection to AACT database.")
        return con
        

    @staticmethod
    def return_connection(connection):        
        AACT.__connection_pool.putconn(connection)
        print("Returned connection to AACT database.")

    @staticmethod
    def close_all_connections():        
        AACT.__connection_pool.closeall()
        print("Closed all connections to AACT database.")

# -----------------------------------------------------------------------------#
class AACTDao:
    def __init__(self):
        AACT.initialise()
        print("Acquiring connection")
        self.connection = AACT.get_connection()

    def __del__(self):
        print("Closing db connection")        
        self.connection.close()

    def read_table(self, table=None):
       
        cursor = self.connection.cursor()
        try:
            cursor.execute(sql.SQL("SELECT * FROM {schema}.{table}").format(schema=sql.Identifier("ctgov"),
                                                                            table=sql.Identifier(table)))
        except (Exception, DatabaseError) as error:
            print(error)
            cursor.close()
            return 1
            
        data = cursor.fetchall()        
        cursor.close()
        # Obtain column names. 
        query_generator = Queryator()
        query = query_generator.columns(table)
        column_names = self.read_query(query)        
        # Convert the postgresql data to dataframe format.
        df = pd.DataFrame(data, columns=column_names)
        return df
        
    def read_query(self, query):
        """Read SQL Table
            Args:
              query: text, query to read table or multiple tables
            Returns:
              pandas dataframe, if query is valid
            """
        df = pd.read_sql_query(sql=query, con=self.connection)
        return df    
# -----------------------------------------------------------------------------#
class AACTCursor:
    def __init__(self):
        self.connection = None
        self.cursor = None

    def __enter__(self):
        self.connection = AACT.get_connection()
        self.cursor = self.connection.cursor()
        return self.cursor

    def __exit__(self, exception_type, exception_value, exception_traceback):
        if exception_value:  
            self.connection.rollback()
        else:
            self.cursor.close()
            self.connection.commit()
        AACT.return_connection(self.connection)
