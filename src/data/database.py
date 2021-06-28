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
# Modified : Monday, June 28th 2021, 12:10:04 am                               #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#
import os
import shutil
from psycopg2 import connect, pool, sql, DatabaseError
import pandas as pd
import numpy as np

from configs.config import DataSourceConfig, get_config

class DBCon:

    __connection_pool = None

    @staticmethod
    def initialise(datasource):
        print("Initializing database. Defaults to the AACT database")
        credentials = DataSourceConfig.get_config(datasource=datasource)
        DBCon.__connection_pool = pool.SimpleConnectionPool(1, 10, **credentials)

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
    def __init__(self, schema='ctgov', config_section='aactdb'):
        DBCon.initialise(config_section=config_section)
        print("Acquiring connection")
        self.connection = DBCon.get_connection()        
        self.schema_name = schema
        self.schema = None
        self.table_names = None

    def __del__(self):
        print("Closing db connection")        
        self.connection.close()

    def get_schema(self):
        if self.schema is None:
            query = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE "
            query += "table_schema = '{schema}' ORDER BY table_name;".format(schema=self.schema_name)            
            self.schema = pd.read_sql_query(query, con=self.connection)

    def get_table_names(self):
        if self.table_names is None:
            query = "SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE "
            query += "table_schema = '{schema}'".format(schema=self.schema_name)
            self.table_names = list(pd.read_sql_query(query, con=self.connection)["table_name"].values)

            # Remove internal metadata. This is some Rails or Oracle related database object.
            self.table_names.remove("ar_internal_metadata")
        return self.table_names

    def get_columns(self, table, dtype=None):
        self.get_schema()
        columns = self.schema.loc[(self.schema["table_name"]==table)]
        if dtype == "date":
            columns = columns.loc[(columns["column_name"].str.endswith("date")) | 
                                    (columns["column_name"].str.endswith("at"))]
        columns = list(np.array(columns["column_name"].values).flatten())
        if len(columns) == 0:
            columns = None
        return columns

        
    def read_table(self, table, idx=None, coerce_float=True, parse_dates=None):
        query = "SELECT * FROM {schema}.{table};".format(schema=self.schema_name,table=table)
        df = pd.read_sql_query(sql=query, con=self.connection, index_col=idx, 
                                coerce_float=coerce_float, parse_dates=parse_dates)
        return df    

class DatasetBuilder:    
    '''DatasetBuilder: Builds datasets as csv files from Postgresql databases.     
    '''
    def __init__(self):
        pass

    def get_destination(self, destination):
        '''Creates the destination directory if it doesn't already exist, then returns the path'''
        destination = os.path.join(get_config(section="directories")["raw"], destination)        
        if not os.path.exists(destination):
            os.makedirs(destination)
        return destination


    def make_dataset(self, dao, table, destination='aact', force=False):
        destination = self.get_destination(destination)
        filename = table + ".csv"
        filepath = os.path.join(destination, filename)

        if force == False and os.path.exists(filepath):            
            print("File {filepath} exists. To overwrite, set force=True".format(filepath=filepath))
            return 1
        date_columns = dao.get_columns(table, dtype="date")
        df = dao.read_table(table=table, parse_dates=date_columns)
        df.to_csv(filepath)            
        print("{table} processed.".format(table=table))

    def make_datasets(self, dao, destination='aact', force=False):
        '''Extracts tables from the dao database and stores as .csv files'''
        
        destination = self.get_destination(destination)        
        tables = dao.get_table_names()
        ntables = len(tables)
        n = 0
        for table in tables:
            n += 1
            print("Processing {table}: {n} of {ntables}".format(table=table, n=n,ntables=ntables))
            filename = table + ".csv"
            filepath = os.path.join(destination,filename)
            if (os.path.exists(filepath) and force == True) or (not os.path.exists(filepath)):            
                date_columns = dao.get_columns(table, dtype="date")          
                df = dao.read_table(table=table, parse_dates=date_columns)
                df.to_csv(filepath)
        print("Processed {n} tables".format(n=n))



    
        

