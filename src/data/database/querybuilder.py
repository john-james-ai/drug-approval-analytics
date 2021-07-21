#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#==============================================================================#
# Project  : Drug Approval Analytics                                           #
# Version  : 0.1.0                                                             #
# File     : \src\data\database\querygen.py                                    #
# Language : Python 3.9.5                                                      #
# -----------------------------------------------------------------------------#
# Author   : John James                                                        #
# Company  : nov8.ai                                                           #
# Email    : john.james@nov8.ai                                                #
# URL      : https://github.com/john-james-sf/drug-approval-analytics          #
# -----------------------------------------------------------------------------#
# Created  : Monday, July 19th 2021, 2:26:36 pm                                #
# Modified : Tuesday, July 20th 2021, 12:24:29 am                              #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#
"""Query Template Generator."""
from abc import ABC, abstractmethod
import psycopg2
from psycopg2 import sql
# -----------------------------------------------------------------------------#
class QueryBuilder(ABC):
    """Generates sql query templates for parameterized pyscopg2 SQL statements. """

    def __init__(self, name=None):        
        self.reset()
        self._name = name

    @abstractmethod
    def reset(self):
        pass

    def set_name(self, name):
        self._name = name
        return self
        
    @abstractmethod
    def get(self):
        pass
# -----------------------------------------------------------------------------#
#                          DATABASE ADMIN                                      #    
# -----------------------------------------------------------------------------#
class CreateDatabase(QueryBuilder):

    def __init__(self, name=None):
        super(CreateDatabase,self).__init__(name)    

    def reset(self):
        self._name = None

    def get(self):
        return sql.SQL("CREATE DATABASE {}").format(sql.Identifier(self._name))

# -----------------------------------------------------------------------------#
class DropDatabase(QueryBuilder):

    def __init__(self, name=None):
        super(DropDatabase,self).__init__(name)    

    def reset(self):
        self._name = None

    def get(self):
        return sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(self._name))

# -----------------------------------------------------------------------------#
class CreateTable(QueryBuilder):

    def __init__(self, name=None):
        super(CreateTable,self).__init__(name)

    def reset(self):
        self._name = None
        self._columns = {}
        self._references = {}
        self._foreign_keys = {}
        self._primary_key = []
        self._sql_command = None
        return self

    def add_column(self, name, datatype, nullable=True, unique=False, default=None):
        self._columns[name] = {}
        self._columns[name]['datatype'] = datatype
        self._columns[name]['nullable'] = nullable
        self._columns[name]['unique'] = unique
        self._columns[name]['default'] = default
        return self

    def set_primary_key(self, columns=None):
        if columns:
            if isinstance(columns, str):
                self._primary_key.append(columns)
            elif isinstance(columns, list):
                self._primary_key = columns
            else:
                logger.error("Primary key must be a string column name or list of column names")
                raise TypeError


    def add_foreign_key(self, name, reftable, refcolumn):
        self._foreign_keys[name] = {}
        self._foreign_keys[name]['reftable'] = reftable
        self._foreign_keys[name]['refcolumn'] = refcolumn
        return self

    def _get_command(self):
        self._sql_command = "CREATE TABLE {} (".format(self._name)

    def _get_columns(self):
        cols = []
        for name, properties in self._columns.items():            
            column_properties = []
            column_properties.append("{} {}".format(name, properties['datatype']))
            if not properties['nullable']:
                column_properties.append("NOT NULL")
            if properties['unique']:
                column_properties.append("UNIQUE")
            column_properties = " ".join(column_properties)
            cols.append(column_properties)
        self._sql_command += ", ".join(cols)

    def _get_primary_key(self):
        if len(self._primary_key) > 0:
            pk = ", ".join(self._primary_key)
            self._sql_command += "PRIMARY KEY ({})".format(pk)

    def _get_foreign_key(self):
        if len(self._foreign_keys) > 0:
            for name, params in self._foreign_keys.items():                
                self._sql_command += "CONSTRAINT {} ".format(name)
                self._sql_command += "FOREIGN KEY ({}) ".format(params['refcolumn'])
                self._sql_command += "REFERENCES {}({}) ".format(params['reftable'],params['refcolumn'])

    def _get_closing(self):
        self._sql_command += ");"
        
    def get(self):
        self._get_command()
        self._get_columns()
        self._get_primary_key()
        self._get_foreign_key()
        self._get_closing()
        return sql.SQL(self._sql_command)

# -----------------------------------------------------------------------------#
class DropTable(QueryBuilder):

    def __init__(self, name=None):
        super(DropTable, self).__init__(name)

    def reset(self):
        self._name = None
        return self        

    def get(self):
        self._sql_command = sql.SQL(
            """DROP TABLE IF EXISTS {}""").format(
                sql.Identifier(self._name))
        return self._sql_command
        
# -----------------------------------------------------------------------------#
class TableExists(QueryBuilder):

    def __init__(self, name=None):
        super(TableExists, self).__init__(name)

    def reset(self):
        self._name = None
        return self        

    def get(self):
        self._sql_command = sql.SQL(
            """SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = %s)""")
        return self._sql_command
    

