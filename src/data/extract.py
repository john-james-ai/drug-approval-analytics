#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#==============================================================================#
# Project  : Predict-FDA                                                       #
# Version  : 0.1.0                                                             #
# File     : \extract.py                                                       #
# Language : Python 3.9.5                                                      #
# -----------------------------------------------------------------------------#
# Author   : John James                                                        #
# Company  : nov8.ai                                                           #
# Email    : john.james@nov8.ai                                                #
# URL      : https://github.com/john-james-sf/predict-fda                      #
# -----------------------------------------------------------------------------#
# Created  : Sunday, June 27th 2021, 5:00:34 pm                                #
# Modified : Monday, June 28th 2021, 1:13:24 am                                #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#
from abc import ABC, abstractmethod
import os, requests
import shutil
from urllib.request import urlopen
from pathlib import PureWindowsPath

from psycopg2 import connect, pool, sql, DatabaseError

import pandas as pd
import numpy as np
from io import BytesIO
from zipfile import ZipFile
from bs4 import BeautifulSoup

from configs.config import DataSourceConfig, DirectoryConfig
from src.data.database import DBCon

# -----------------------------------------------------------------------------#
class Extractor(ABC):
    """Base class for Data Extractors."""

    def __init__(self, datasource, **kwargs):
        self.datasource = datasource
        
    @abstractmethod
    def enumarate_datasource(self):
        pass

    @abstractmethod
    def extract(self, force=False):
        pass

# -----------------------------------------------------------------------------#
class WebExtractor(Extractor):
    """Extracts data from a website by means of scraping"""

    def __init__(self, datasource):
        self.datasource = datasource
        self.schema = schema
        DBCon.initialise(datasource=datasource)
        self.connection = DBCon.get_connection()
        print("Acquiring connection")
        self.connection = DBCon.get_connection()      

    def __del__(self):
        print("Closing db connection")        
        self.connection.close()        

    def enumerate_datasource(self):
        """Returns a list of tables in the datasource"""
        
        query = "SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE "
        query += "table_schema = '{schema}'".format(schema=self.schema)      
        tables = list(pd.read_sql_query(query, con=self.connection)["table_name"].values)  
        # Remove this internal table from the data sample. Used for Rails, or Oracle.
        tables.remove("ar_internal_metadata")
        return tables


    def extract(self, force=False):
        directory_config = DirectoryConfig()
        destination = directory_config.get_config(self.datasource)

        tables = self.enumerate_datasource()
        ntables = len(tables)
        n = 0
        for table in tables:
            n += 1
            print("Processing {table}: {n} of {ntables}".format(table=table, n=n,ntables=ntables))
            filename = table + ".csv"
            filepath = PureWindowsPath(destination).joinpath(filename)
            if (os.path.exists(filepath) and force == True) or (not os.path.exists(filepath)):                            
                df = DBCon.read_table(table=table)
                df.to_csv(filepath)
        print("Processed {n} tables".format(n=n))            




