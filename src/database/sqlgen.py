#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#==============================================================================#
# Project  : Predict-FDA                                                       #
# Version  : 0.1.0                                                             #
# File     : \sql.py                                                           #
# Language : Python 3.9.5                                                      #
# -----------------------------------------------------------------------------#
# Author   : John James                                                        #
# Company  : nov8.ai                                                           #
# Email    : john.james@nov8.ai                                                #
# URL      : https://github.com/john-james-sf/predict-fda                      #
# -----------------------------------------------------------------------------#
# Created  : Wednesday, June 23rd 2021, 12:25:51 pm                            #
# Modified : Saturday, June 26th 2021, 5:30:28 pm                              #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#
from psycopg2 import sql

class Queryator:

    def __init__(self, style="postgresql"):
        self.style = style

    def query(self, table):
        if self.style == "postgresql":
            q = sql.SQL("SELECT * FROM ctgov.{table};".format(table=sql.Identifier(table))) 
        else:
            q = "SELECT * FROM ctgov.{table};".format(table=table)
        return q

    def vslice(self, table, columns):
        cols = ",".join(columns)
        return "SELECT {cols} FROM ctgov.{table};".format(cols=cols,table=table)

    def hslice(self, table, condition):
        return "SELECT * FROM ctgov.{table} WHERE {condition};".format(table=table, condition=condition)

    def subset(self, table, columns, condition):
        cols = ",".join(columns)
        return "SELECT {cols} FROM ctgov.{table} WHERE {condition};".format(cols=cols, table=table, condition=condition)        

    def columns(self, table):
        s = "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE "
        s += "table_schema = 'ctgov' AND table_name = '{table}';".format(table=table)
        return s        
        
    def tables(self):
        s = "SELECT table_name FROM information_schema.tables"
        s += " WHERE ( table_schema = 'ctgov' );"
        return s

    def nrows(self, table, what="*"):
        s = "SELECT COUNT({what}) FROM ctgov.{table};".format(what=what, table=table)
        return s

    def ncols(self, table):
        s = "SELECT count(column_name) FROM INFORMATION_SCHEMA.COLUMNS WHERE "
        s += "table_schema = 'ctgov' AND table_name = '{table}';".format(table=table)
        return s                
