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
# Modified : Monday, July 19th 2021, 5:37:22 pm                                #
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
import shlex

from ...utils.config import DBConfig
      
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
        try:
            # Obtain the database credentials from configuration file.
            config = DBConfig()
            credentials = config('postgres')

            # Establish a connection to the database and set the commit isolationlevel.
            con = psycopg2.connect(**credentials)
            con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

            # Obtain a cursor and execute the sql commands.
            cur = con.cursor()
            cur.execute("DROP DATABASE IF EXISTS {} ;".format(dbname))        
            cur.execute("CREATE DATABASE {} ;".format(dbname))
            cur.execute("GRANT ALL PRIVILEGES ON DATABASE {} TO {} ;".format(dbname, credentials['user']))

            # Close the cursor and commit the changes.
            cur.close()
            con.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            logger.error(error)

        finally:
            if con is not None:
                con.close()

        logger.info("Database {} created successfully.".format(dbname))
        
        return dbname

    def drop_database(self, dbname):
        """Drops database if it exists.

        Parameters
        ----------
        dbname (str): the name of the database as defined in the credentials file.

        """        
        try:
            # Obtain the database credentials from configuration file.
            config = DBConfig()
            credentials = config('postgres')

            # Establish a connection to the database and set the commit isolationlevel.
            con = psycopg2.connect(**credentials)
            con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

            # Obtain a cursor and execute the sql commands.
            cur = con.cursor()
            cur.execute("DROP DATABASE IF EXISTS {} ;".format(dbname))     

            # Close the cursor and commit the changes.
            cur.close()
            con.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            logger.error(error)

        finally:
            if con is not None:
                con.close()

        logger.info("Database {} created deleted.".format(dbname))

        con.close()
        return dbname 


    def database_exists(self, dbname):
        """Prints database version if it exists.

        Parameters
        ----------
        dbname (str): the name of the database as defined in the credentials file.

        """ 
        try:
            # Obtain the database credentials from configuration file.
            config = DBConfig()
            credentials = config(dbname)

            # Establish a connection to the database and set the commit isolationlevel.
            con = psycopg2.connect(**credentials)

            # Obtain a cursor and execute the sql commands.
            cur = con.cursor()
            cur.execute("SELECT version()")     
            version = cur.fetchone()
            print(version)

            # Close the cursor and commit the changes.
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            logger.error(error)

        finally:
            if con is not None:
                con.close()
                logger.info("Database connection closed.")

        return version


    def create_table(self, dbname, tablename, sql_command):

        try:
            # Obtain the database credentials from configuration file.
            config = DBConfig()
            credentials = config(dbname)

            # Establish a connection to the database and set the commit isolationlevel.
            con = psycopg2.connect(**credentials)
            con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

            # Obtain a cursor and execute the sql commands.
            cur = con.cursor()
            cur.execute(sql_command)        

            # Close the cursor and commit the changes.
            cur.close()
            con.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            logger.error(error)

        finally:
            if con is not None:
                con.close()        
        
        logger.info("Table {} created.".format(tablename))

    # TODO
    def backup(self, dbname, backup_filepath):
        """Drops database if it exists.

        Parameters
        ----------
        dbname (str): the name of the database as defined in the credentials file.
        backup_filepath (str): the backup relative filepath. 

        """             
        config = DBConfig()
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
        config = DBConfig()
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
