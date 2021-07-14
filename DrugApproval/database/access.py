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
# Modified : Monday, July 12th 2021, 2:52:04 am                                #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#
import os
import shutil
from datetime import datetime
from sys import exit

from subprocess import Popen, PIPE
import psycopg2
from psycopg2 import connect, pool, sql
import pandas as pd
import numpy as np
import shlex

from config.config import Configuration, Credentials, Config, AACTConfig
from approval.logging import Logger, exception_handler, logging_decorator
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
Base = declarative_base()
# -----------------------------------------------------------------------------#
#                       POSTGRES DATABASE CONNECTION                           #
# -----------------------------------------------------------------------------# 
class DBCon:

    __connection_pool = None

    @staticmethod
    def initialise(credentials):         
        DBCon.__connection_pool = pool.SimpleConnectionPool(2, 10, **credentials)

    @staticmethod
    def get_connection():        
        con =  DBCon.__connection_pool.getconn()
        return con
        

    @staticmethod
    def return_connection(connection):        
        DBCon.__connection_pool.putconn(connection)

    @staticmethod
    def close_all_connections():        
        DBCon.__connection_pool.closeall()


# -----------------------------------------------------------------------------#
#                     DATA ACCESS OBJECT FOR POSTGRES DATABASES                #
# -----------------------------------------------------------------------------#
class DBDao:
    def __init__(self, dbname='aact'):                
        self._dbname = dbname
        self._credentials = Config().get(dbname +'_credentials')
        self._configuration = AACTConfig(dbname)
        DBCon.initialise(self._credentials) 
        self._connection = DBCon.get_connection()                
        self._schema_name = self._configuration.schema_name    
        
    def __del__(self):   
        self._connection.close()

    @property
    def tables(self):        
        """Returns a list of tables in the currently open database.

        Returns
        -------
        list
            Table names in the currently open database
        """  
        
        query = "SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE "
        query += "table_schema = '{schema}';".format(schema=self._schema_name)
        tables = list(pd.read_sql_query(query, con=self._connection)["table_name"].values)

        # Remove internal metadata. This is some Rails or Oracle related database object.
        tables.remove("ar_internal_metadata")
        return tables        

    def get_columns(self, table):
        """Returns the list of columns and data types for the designated table

        Parameters
        ----------
        table : str
            The name of the table for which the columns are being requested.

        Returns
        -------
        DataFrame
            Columns and their datatypes for the table requested.
        """        
        query = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS "
        query += " WHERE table_schema = '{schema}' AND table_name = '{table}';"\
            .format(schema=self._schema_name, table=table)        
        columns = pd.read_sql_query(query, con=self._connection)
        return columns    
            
        
    def read_table(self, table, idx=None, coerce_float=True, parse_dates=None):
        query = "SELECT * FROM {schema}.{table};".format(schema=self._schema_name,table=table)
        df = pd.read_sql_query(sql=query, con=self._connection, index_col=idx, 
                                coerce_float=coerce_float, parse_dates=parse_dates)
        return df    


# -----------------------------------------------------------------------------#
#                       DATABASE ADMINISTRATION                                #
# -----------------------------------------------------------------------------#
class DBAdmin:

    @exception_handler
    def create_database(self, credentials):
        """Creates a new database. If it already exists, its deleted.

        Parameters
        ----------
        credentials : dict
            Dictionary containing the host, database, user id, password and port

        Returns
        -------
        str
            The name of the database created.
        """        
        con = psycopg2.connect(**credentials)
        cur = con.cursor()
        cur.execute("DROP DATABASE {} ;".format(credentials.database))        
        cur.execute("CREATE DATABASE {} ;".format(credentials.database))
        cur.execute("GRANT ALL PRIVILEGES ON DATABASE {} TO {} ;".format(credentials.database, credentials.user))
        return credentials.database  

    @logging_decorator
    @exception_handler
    def drop_database(self, postgres_credentials, database_credentials):
        """Drops database if it exists.

        Parameters
        ----------
        postgres_credentials : class
            Class containing the DEFAULT POSTGRES credentials 
        database_credentials : class
            Class containing the credentials for the database being checked.             
        """        
        connection = None
        connection = psycopg2.connect(**postgres_credentials)    
        cursor = connection.cursor()
        cursor.execute("""DROP DATABASE IF EXISTS %s""",(database_credentials.database))       


    @logging_decorator
    @exception_handler
    def backup(self, configuration):
        """Backup postgres database to file.

        Parameters
        ----------
        configuration : class
            Class containing the host, database, user id, password and port
        """                
        backup_filepath = configuration.backup_filepath.format(date=datetime.now().strftime("%Y%m%d"))
        process = Popen(
            ['pg_dump',
            '--dbname=postgresql://{}:{}@{}:{}/{}'.format(
                configuration.credentials['user'],
                configuration.credentials['password'],
                configuration.credentials['host'],
                configuration.credentials['port'],
                configuration.credentials['database']),
            '-f', configuration.backup_filepath],
            stdout=PIPE
        )
        output = process.communicate()[0]
        if process.returncode != 0:
            print('Command failed. Return code : {}'.format(process.returncode))
            exit(1)
        return output

            
    @logging_decorator
    @exception_handler
    def restore(self, configuration):
        """Restore postgres from file

        This is also used to refresh the database with the latest
        export from the source website.  It is advised to BACKUP THE ORIGINAL
        before restoring.

        Parameters
        ----------
        configuration : class
            Class containing credentials and  filename for restoration
        """      
        
        restore_filepath = configuration.filepath
        restore_cmd = "pg_dump -e -v -O -x -h {host} -d {database} \
                        -U {user} -p {port} --clean --no-owner -Fc -f {filepath}"\
                        .format(configuration.host, configuration.database,
                                configuration.user, configuration.port,
                                restore_filepath)

        restore_cmd = shlex.split(restore_cmd)

        process = Popen(restore_cmd, shell=False, stdin=PIPE, 
                        stdout=PIPE, stderr=PIPE)            

        output = process.communicate('{}n'.format(configuration.password))[0]
        if int(process.returncode) != 0:
            print('Command failed. Return code : {}'.format(process.returncode))

        return output
        

    def promote_database(active_db_credentials, temp_db_credentials):
        """Promotes a temporary database to active.

        Parameters
        ----------
        active_db_credentials : dict
            Database, user, password, host, and port for active database
        temp_db_credentials : [type]
            Database, user, password, host, and port for temp database
        """        

        con = psycopg2.connect(**active_db_credentials)
        cur = con.cursor()
        cur.execute("SELECT pg_terminate_backend( pid ) "
                    "FROM pg_stat_activity "
                    "WHERE pid <> pg_backend_pid( ) "
                    "AND datname = '{}'".format(active_db_credentials.database))
        cur.execute("DROP DATABASE {}".format(active_db_credentials.database))
        cur.execute('ALTER DATABASE "{}" RENAME TO "{}";'.format(temp_db_credentials.database, 
                                                                active_db_credentials.database))
