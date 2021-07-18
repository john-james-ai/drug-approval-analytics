#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#==============================================================================#
# Project  : Predict-FDA                                                       #
# Version  : 0.1.0                                                             #
# File     : \database.py                                                      #
# Language : Python 3.9.5                                                      #
# -----------------------------------------------------------------------------#
# Author   : John James                                                        #
# Company  : nov8.ai                                                           #
# Email    : john.james@nov8.ai                                                #
# URL      : https://github.com/john-james-sf/predict-fda                      #
# -----------------------------------------------------------------------------#
# Created  : Thursday, July 15th 2021, 11:58:44 am                             #
# Modified : Saturday, July 17th 2021, 10:23:18 pm                             #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#
import os
import shutil
from datetime import datetime
from sys import exit
import logging
logger = logging.getLogger(__name__)

from subprocess import Popen, PIPE
import psycopg2
from psycopg2 import connect, pool, sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import pandas as pd
import shlex

from ..utils.config import Credentials
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
    def __init__(self, dbname):                
        self._dbname = dbname
        credentials = Credentials()
        self._credentials = credentials(dbname)
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
#                       POSTGRSE DATABASE ADMINISTRATION                       #
# -----------------------------------------------------------------------------#
class DBAdmin:

    def create_database(self, dbname):
        """Creates a new database. If it already exists, its deleted.

        Parameters
        ----------
        dbname (str): the name of the database as defined in the credentials file.

        Returns
        -------
        str
            The name of the database created.
        """        
        # Postgres api connect format 
        # engine = create_engine(
        #     "postgresql://user:pass@hostname/dbname",
        #     connect_args={"connection_factory": MyConnectionFactory}
        # )
        config = Credentials()
        credentials = config('postgres')

        con = psycopg2.connect(**credentials)
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        cur = con.cursor()
        cur.execute("DROP DATABASE IF EXISTS {} ;".format(dbname))        
        cur.execute("CREATE DATABASE {} ;".format(dbname))
        cur.execute("GRANT ALL PRIVILEGES ON DATABASE {} TO {} ;".format(dbname, credentials['user']))

        logger.info("Database {} created successfully.".format(dbname))

        con.close()
        return dbname

    def drop_database(self, dbname):
        """Drops database if it exists.

        Parameters
        ----------
        dbname (str): the name of the database as defined in the credentials file.

        """        
        
        config = Credentials()
        credentials = config('postgres')

        con = psycopg2.connect(**credentials)
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        cur = con.cursor()
        cur.execute("DROP DATABASE IF EXISTS {} ;".format(dbname))        

        logger.info("Database {} created deleted.".format(dbname))

        con.close()
        return dbname 


    # TODO
    def backup(self, dbname, backup_filepath):
        """Drops database if it exists.

        Parameters
        ----------
        dbname (str): the name of the database as defined in the credentials file.
        backup_filepath (str): the backup relative filepath. 

        """             
        config = Credentials()
        credentials = config(dbname)

        backup_filepath = backup_filepath.format(date=datetime.now().strftime("%Y%m%d"))
        process = Popen(
            ['pg_dump',
            '--dbname=postgresql://{}:{}@{}:{}/{}'.format(
                credentials['user'],
                credentials['password'],
                credentials['host'],
                credentials['port'],
                credentials['dbname']),
            '-f', backup_filepath],
            stdout=PIPE
        )
        output = process.communicate()[0]
        if process.returncode != 0:
            print('Command failed. Return code : {}'.format(process.returncode))
            exit(1)
        return output

    #TODO 
    def restore(self, dbname, restore_filepath):
        """Restore postgres from file

        Parameters
        ----------
        dbname (str): the name of the database as defined in the credentials file.
        restore_filepath (str): the filepath from which the database will be restored. 

        """  
        config = Credentials()
        credentials = config(dbname)
        
        restore_cmd = "pg_dump -e -v -O -x -h {host} -d {database} \
                        -U {user} -p {port} --clean --no-owner -Fc -f {filepath}"\
                        .format(credentials.host, credentials.database,
                                credentials.user, credentials.port,
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
