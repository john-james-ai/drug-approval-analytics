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
# Modified : Wednesday, June 30th 2021, 10:59:26 am                            #
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

from configs.config import Config
from src.logging import Logger

class DBCon:

    __connection_pool = None

    @staticmethod
    def initialise(database):
        print("Initializing database. Defaults to the AACT database")
        credentials = Config.get_config(section=database + '_credentials')
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
#                         DATABASE BACKUP RESTORE                              #
# -----------------------------------------------------------------------------#        
class DBAdmin:
    """PostgreSQL administation."""
    def __init__(self, database='aact'):
        logo = Logger(__name__)
        self._logger = logo.get_logger()
        self._logger.info("Instantiating DBAdmin")
        self._database = database
        self._config = Config().get_config(database)
        self._credentials = Config().get_config(database + '_credentials')
        self._project_dir = Config().get_config('basedirectories')['project_dir']
        self._database = self._credentials['database']
        self._user = self._credentials['user']
        self._password = self._credentials['password']
        self._host = self._credentials['host']
        self._port = self._credentials['port']
        backup_dir = self._config['backup_dir']
        self._now = datetime.now().strftime("%Y-%m-%d %H:%M:%S") 
        backup_filename = self._database + '_' + self._now + '.dump'
        self._backup_filepath = os.path.join(backup_dir, backup_filename)

      

    def backup(self):
        """Performs backup of postgres database
        """
        self._now = datetime.now().strftime("%Y-%m-%d %H:%M:%S") 
        self._logger.info("Requesting backup of {db} at {dt}".format(db=self._database, dt=self._now))

        if not os.path.exists(os.path.dirname(self._backup_filepath)):
            os.makedirs(self._backup_filepath)

        command = f'pg_dump --host={self._host} --dbname={self._database} '\
                  f'--username={self._user} --password={self._password} '\
                      f'--file={self._backup_filepath}'
        try:
            process = Popen(command, stdout=PIPE, stderr=PIPE, shell=True)   
        except SubprocessError as e:
            self._logger.error("Houston. We have a problem opening the process. {e}".format(e)) 

        try:
            process.communicate('{}\n'.format(self._password))      
            if int(process.returncode) != 0:
                self._logger.error("Backup command failed with return code: {}".format(process.returncode)) 
        except SubprocessError as e:
            self._logger.error("Houston. We have with the backup! {e}".format(e))            
            
        self._now = datetime.now().strftime("%Y-%m-%d %H:%M:%S") 
        self._logger.info("Completed backup of {db} at {dt}".format(db=self._database, dt=self._now))

    def restore(self):
        restore_dir = self._config['download_dir']
        restore_filename = self._config['restore_filename']
        restore_filepath = os.path.join(restore_dir, restore_filename).replace("\\","/")
        self._restore_command = "pg_restore -h " + self._command + " -v " +\
                                restore_filepath

        try:
            proc = Popen(self._restore_command, stdout=PIPE, stderr=PIPE)
            stdout, stderr = proc.communicate()
            print("Restore complete!")
        except SubprocessError as e:
            print("Houston. We have a problem!")
            print(e)                                
                    




        



# -----------------------------------------------------------------------------#
#                     DATA ACCESS OBJECT BASE CLASS                            #
# -----------------------------------------------------------------------------#
class DBDao:
    def __init__(self, schema='ctgov', database='aact'):
        self._logger = Logger()
        DBCon.initialise(database=database)
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





    
        

