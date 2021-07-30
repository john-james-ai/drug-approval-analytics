#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# =========================================================================== #
# Project  : Drug Approval Analytics                                          #
# Version  : 0.1.0                                                            #
# File     : \src\data\database\admin.py                                      #
# Language : Python 3.9.5                                                     #
# --------------------------------------------------------------------------  #
# Author   : John James                                                       #
# Company  : nov8.ai                                                          #
# Email    : john.james@nov8.ai                                               #
# URL      : https://github.com/john-james-sf/drug-approval-analytics         #
# --------------------------------------------------------------------------  #
# Created  : Monday, July 19th 2021, 1:39:19 pm                               #
# Modified : Thursday, July 29th 2021, 3:10:52 pm                             #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #

from datetime import datetime
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from subprocess import Popen, PIPE
# import shlex
from sys import exit

from ...utils.logger import exception_handler, logger
from ...utils.config import DBConfig

# --------------------------------------------------------------------------- #
#                       POSTGRES DATABASE ADMINISTRATION                      #
# --------------------------------------------------------------------------- #


class DBAdmin:

    @exception_handler()
    def create_database(self, dbname: str) -> str:
        """Creates a new database. If it already exists, its deleted.

        Arguments:
            dbname (str): the name of the database

        Returns:
            str: The name of the database created.

        """
        # Note the exception handling is managed by the exception handler
        # decorator

        # Obtain the database credentials from configuration file.
        config = DBConfig()
        credentials = config('postgres')

        # Establish a database connection and set the commit isolationlevel.
        con = psycopg2.connect(**credentials)
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        # Obtain a cursor and execute the sql commands.
        cur = con.cursor()
        cur.execute("DROP DATABASE IF EXISTS {} ;".format(dbname))
        cur.execute("CREATE DATABASE {} ;".format(dbname))
        cur.execute("GRANT ALL PRIVILEGES ON DATABASE {} TO {} ;".format(
            dbname, credentials.user))

        # Close the cursor and commit the changes.
        cur.close()
        con.commit()
        con.close()

        logger.info("Database {} created successfully.".format(dbname))

        return dbname

    @exception_handler()
    def drop_database(self, dbname: str) -> None:
        """Drops database if it exists.

        Arguments:
            dbname (str): the name of the database

        """
        # Note the exception handling is managed by the exception handler
        # decorator

        # Obtain the database credentials from configuration file.
        config = DBConfig()
        credentials = config('postgres')

        # Establish a database connection and set the commit isolationlevel.
        con = psycopg2.connect(**credentials)
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        # Obtain a cursor and execute the sql commands.
        cur = con.cursor()
        cur.execute("DROP DATABASE IF EXISTS {} ;".format(dbname))

        # Close the cursor and commit the changes.
        cur.close()
        con.commit()
        con.close()

        logger.info("Database {} dropped successfully.".format(dbname))

    @exception_handler()
    def database_exists(self, dbname: str) -> None:
        """Prints database version if it exists.

        Arguments:
            dbname (str): the name of the database

        """
        # Note the exception handling is managed by the exception handler
        # decorator

        # Obtain the database credentials from configuration file.
        config = DBConfig()
        credentials = config(dbname)

        # Establish a database connection and set the commit isolationlevel.
        con = psycopg2.connect(**credentials)

        # Obtain a cursor and execute the sql commands.
        cur = con.cursor()
        cur.execute("SELECT version()")
        version = cur.fetchone()
        print(version)

        # Close the cursor and commit the changes.
        cur.close()
        con.close()

        return version

    @exception_handler()
    def create_table(self, dbname: str, tablename: str,
                     sql_command: str) -> tuple:

        # Obtain the database credentials from configuration file.
        config = DBConfig()
        credentials = config(dbname)

        # Establish a database connection and set the commit isolationlevel.
        con = psycopg2.connect(**credentials)
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        # Obtain a cursor and execute the sql commands.
        cur = con.cursor()
        cur.execute(sql_command)

        # Close the cursor and commit the changes.
        cur.close()
        con.commit()
        con.close()

        logger.info("Table {} created.".format(tablename))

    @exception_handler()
    def backup(self, dbname: str, backup_filepath: str) -> tuple:
        """Drops database if it exists.

        Arguments:
            dbname (str): the name of the database
            backup_filepath (str): the backup relative filepath.

        """
        config = DBConfig()
        credentials = config(dbname)

        backup_filepath = backup_filepath.format(
            date=datetime.now().strftime("%Y%m%d"))
        process = Popen(
            ['pg_dump',
             '--dbname=postgresql://{}:{}@{}:{}/{}'.format(
                 credentials.user,
                 credentials.password,
                 credentials.host,
                 credentials.port,
                 credentials.dbname),
             '-f', backup_filepath],
            stdout=PIPE
        )
        output = process.communicate()[0]
        if process.returncode != 0:
            print('Command failed. Return code : {}'
                  .format(process.returncode))
            exit(1)
        return output

    # @exception_handler()
    # def restore(self, dbname: str, restore_filepath: str) -> tuple:
    #     """Restore postgres from file

    #     Arguments:
    #         dbname (str): the name of the database
    #         restore_filepath (str): the location of the restore file.

    #     """
    #     config = DBConfig()
    #     credentials = config(dbname)

        # restore_cmd = "pg_dump -e -v -O -x -h {host} -d {database} \
        #                 -U {user} -p {port} --clean --no-owner -Fc -f\
        #                {filepath}" .format(credentials.host,
        #                 credentials.database,
        #                   credentials.user, credentials.port,
        #                         restore_filepath)

    #     # restore_cmd = shlex.split(restore_cmd)

    #     process = Popen(restore_cmd, shell=False, stdin=PIPE,
    #                     stdout=PIPE, stderr=PIPE)

    #     output = process.communicate('{}n'.format(credentials.password))[0]
    #     if int(process.returncode) != 0:
    #         print('Command failed. Return code : {}'
    #               .format(process.returncode))

    #     return output

    @exception_handler()
    def promote_database(active_db_credentials: str,
                         temp_db_credentials: str) -> None:
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
                    "AND datname = '{}'"
                    .format(active_db_credentials.database))
        cur.execute("DROP DATABASE {}"
                    .format(active_db_credentials.database))
        cur.execute('ALTER DATABASE "{}" RENAME TO "{}";'
                    .format(temp_db_credentials.database,
                            active_db_credentials.database))
