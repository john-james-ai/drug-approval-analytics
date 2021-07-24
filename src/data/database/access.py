#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# =========================================================================== #
# Project  : Drug Approval Analytics                                          #
# Version  : 0.1.0                                                            #
# File     : \src\data\database\access.py                                     #
# Language : Python 3.9.5                                                     #
# --------------------------------------------------------------------------  #
# Author   : John James                                                       #
# Company  : nov8.ai                                                          #
# Email    : john.james@nov8.ai                                               #
# URL      : https://github.com/john-james-sf/drug-approval-analytics         #
# --------------------------------------------------------------------------  #
# Created  : Monday, July 19th 2021, 1:39:25 pm                               #
# Modified : Thursday, July 22nd 2021, 12:56:58 pm                            #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
from ...utils.config import DBConfig
from .connection import DBCon
import pandas as pd
import psycopg2
from psycopg2 import sql

from ...utils.logger import exception_handler, logger


# -----------------------------------------------------------------------------#
#                     DATA ACCESS OBJECT FOR POSTGRES DATABASES                #
# -----------------------------------------------------------------------------#

class DBDao:
    def __init__(self, dbname):
        self._dbname = dbname
        config = DBConfig()
        credentials = config(dbname)
        DBCon.initialise(credentials)
        self._connection = DBCon.get_connection()

    def __del__(self):
        DBCon.close_all_connections()

    def load(self, table, df):
        """Batch loads a table from pandas dataframe."""

        # Create a list of tupples from the dataframe values
        tuples = [tuple(x) for x in df.to_numpy()]
        # Comma-separated dataframe columns
        cols = ','.join(list(df.columns))
        # SQL quert to execute
        query = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s)" % (table, cols)

        cursor = None
        try:
            cursor = self._connection.cursor()
            cursor.executemany(query, tuples)
            self._connection.commit()
            cursor.close()
            logger.info("Loaded {} table.".format(table))
        except (Exception, psycopg2.DatabaseError) as error:
            logger.error(error)
            self._connection.rollback()
            cursor.close()
        finally:
            if cursor is not None:
                cursor.close()

    def create(self, table, data):
        """Inserts a data row in the form of a dictionary into a postgres table."""
        columns = ", ".join(data.keys())
        values_placeholders = ", ".join(["%s"] * len(data.keys()))

        sql_query = "INSERT INTO %s ( %s ) VALUES ( %s )" % (
            table, columns, values_placeholders)

        cursor = None
        try:
            cursor = self._connection.cursor()
            cursor.execute(sql_query, list(data.values()))
            self._connection.commit()
            cursor.close()
            logger.info("Inserted data into {} table.".format(table))
        except (Exception, psycopg2.DatabaseError) as error:
            logger.error(error)
            self._connection.rollback()
            cursor.close()
        finally:
            if cursor is not None:
                cursor.close()

    def read(self, table, id_col, id_value):

        sql_query = "SELECT * FROM {table} WHERE {col} = '%s'".format(
            table=table, col=id_col) % (id_value)
        df = pd.read_sql_query(sql=sql_query, con=self._connection)
        return df

    def read_table(self, table, idx=None, coerce_float=True, parse_dates=None):
        query = "SELECT * FROM {table};".format(table=table)
        df = pd.read_sql_query(sql=query, con=self._connection, index_col=idx,
                               coerce_float=coerce_float, parse_dates=parse_dates)
        return df

    def update(self, table, column, value, id_col, id_val):
        """Updates table entry indexed by id with data in dict format."""

        sql_query = sql.SQL("UPDATE {table} SET {col} = %s WHERE {id_col} = %s").format(
            table=sql.Identifier(table),
            col=sql.Identifier(column),
            id_col=sql.Identifier(id_col))

        cursor = None
        try:
            cursor = self._connection.cursor()
            cursor.execute(sql_query, (value, id_val))
            self._connection.commit()
            cursor.close()
            logger.info("Updated {} table.".format(table))
        except (Exception, psycopg2.DatabaseError) as error:
            logger.error(error)
            self._connection.rollback()
            cursor.close()
        finally:
            if cursor is not None:
                cursor.close()

    def delete(self, table, id_col, id_value):
        """Deletes a row from the designated table."""

        sql_query = sql.SQL("DELETE FROM {table} WHERE {id_col} = %s").format(
            table=sql.Identifier(table),
            id_col=sql.Identifier(id_col))

        cursor = None
        try:
            cursor = self._connection.cursor()
            cursor.execute(sql_query, (id_value,))
            self._connection.commit()
            cursor.close()
            logger.info("Deleted data from {} table.".format(table))
        except (Exception, psycopg2.DatabaseError) as error:
            logger.error(error)
            self._connection.rollback()
            cursor.close()
        finally:
            if cursor is not None:
                cursor.close()

    def get_tables(self, schema='public'):
        """Returns a list of tables in the currently open database.

        Returns
        -------
        list
            Table names in the currently open database
        """

        query = "SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE "
        query += "table_schema = '{schema}';".format(schema=schema)
        tables = list(pd.read_sql_query(
            query, con=self._connection)["table_name"].values)

        # Remove internal metadata. This is some Rails or Oracle related database object.
        if "ar_internal_metadata" in tables:
            tables.remove("ar_internal_metadata")
        return tables

    def get_columns(self, table, schema=None, metadata=False):
        if metadata:
            return self._get_columns_and_metadata(table, schema)
        else:
            return self._get_columns_only(table, schema)

    def _get_columns_and_metadata(self, table, schema=None):
        """Returns the list of columns and data types for the designated table

        Parameters
        ----------
        table (str): The name of the table for which the columns are being requested.
        schema (str): The table schema. Optional

        Returns
        -------
        list: Names of the columns in the designated table  
        """
        if schema:
            sql_query = sql.SQL(
                "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE {}.table_name = %s".format(schema))
        else:
            sql_query = sql.SQL(
                "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = %s")

        cursor = None
        try:
            cursor = self._connection.cursor()
            cursor.execute(sql_query, (table,))
            colnames = cursor.fetchall()
            cursor.close()

        except (Exception, psycopg2.DatabaseError) as error:
            logger.error(error)
            cursor.close()

        finally:
            if cursor is not None:
                cursor.close()

        colnames_list = []
        for col in colnames:
            colnames_list.append(col[0])

        return colnames_list

    def _get_columns_only(self, table, schema=None):
        if schema:
            table = schema + '.' + table
            df = self.read_table(table)
        else:
            df = self.read_table(table)
        return df.columns
