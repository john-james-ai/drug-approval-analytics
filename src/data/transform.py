#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#==============================================================================#
# Project  : Predict-FDA                                                       #
# Version  : 0.1.0                                                             #
# File     : \transform.py                                                     #
# Language : Python 3.9.5                                                      #
# -----------------------------------------------------------------------------#
# Author   : John James                                                        #
# Company  : nov8.ai                                                           #
# Email    : john.james@nov8.ai                                                #
# URL      : https://github.com/john-james-sf/predict-fda                      #
# -----------------------------------------------------------------------------#
# Created  : Tuesday, June 29th 2021, 6:55:06 pm                               #
# Modified : Wednesday, June 30th 2021, 1:15:40 pm                             #
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

from configs.config import Config
from logging import Logger

# -----------------------------------------------------------------------------#
class Extractor(ABC):
    """Base class for Data Extractors."""

    def __init__(self, datasource, **kwargs):
        self.datasource = datasource
        self.configuration = Config()

    @abstractmethod
    def extract(self, force=False):
        pass

# -----------------------------------------------------------------------------#
class FTPExtractor(Extractor):
    """Downloads data from FTP sites."""

    def __init__(self, datasource):
        super(ZipExtractor, self).__init__(datasource)
        config = self.configuration.get_config(self.datasource)  
        self.baseurl = config["baseurl"]
        self.destination = config["basedir"]
        self.filenames = self.configuration.get_config(self.datasource + '_files')

    def extract(self, force=False):

        if (os.path.exists(self.destination)) & (force is False):
            return
        
        if not os.path.exists(self.destination):
            os.makedirs(self.destination)
        
        for key, filename in self.filenames.items():
            url = self.baseurl + filename
            print("Downloading data from {url}".format(url=url), end=" ")
            results = requests.get(url)
            zipfile = ZipFile(BytesIO(results.content))
            zipfile.extractall(self.destination)
            print("...complete!")
        print("File download complete.")

# -----------------------------------------------------------------------------#
class WebExtractor(Extractor):
    """Extracts data via scraping"""

    def __init__(self, datasource):
        super(WebExtractor, self).__init__(datasource)
        self.config = self.configuration.get_config(self.datasource)  
        self.baseurl = self.config["baseurl"]
        self.downloadurl = self.config["downloadurl"]
        self.destination = self.config["basedir"]

    def _extractfile(self, object):
        link = self.baseurl + object
        print("Downloading data from {url}".format(url=link), end=" ")
        results = urlopen(link)
        zipfile = ZipFile(BytesIO(results.read()))
        zipfile.extractall(self.destination)
        print("...complete!")        


    def extract(self, force=False):
        if (os.path.exists(self.destination)) & (force is False):
            print("Directory exists. To overwrite, set force=True")
            return
        
        if not os.path.exists(self.destination):
            os.makedirs(self.destination)

        page = requests.get(self.downloadurl)
        soup = BeautifulSoup(page.content, 'html.parser')          
        object = eval(self.config["find"]) 
        
        if isinstance(object, str):
            self._extractfile(object)
        elif isinstance(object, (list,tuple)):
            for item in object:
                self._extractfile(item)
         

# -----------------------------------------------------------------------------#
class ZipExtractor(Extractor):
    """Downloads data from static website."""

    def __init__(self, datasource):
        super(ZipExtractor, self).__init__(datasource)
        config = self.configuration.get_config(self.datasource)  
        self.baseurl = config["baseurl"]
        self.destination = config["basedir"]
        self.filenames = self.configuration.get_config(self.datasource + '_files')

    def extract(self, force=False):

        if (os.path.exists(self.destination)) & (force is False):
            return
        
        if not os.path.exists(self.destination):
            os.makedirs(self.destination)
        
        for key, filename in self.filenames.items():
            url = self.baseurl + filename
            print("Downloading data from {url}".format(url=url), end=" ")
            results = requests.get(url)
            zipfile = ZipFile(BytesIO(results.content))
            zipfile.extractall(self.destination)
            print("...complete!")
        print("File download complete.")




