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
# Modified : Wednesday, June 30th 2021, 1:00:12 am                             #
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

# -----------------------------------------------------------------------------#
class Extractor(ABC):
    """Base class for Data Extractors."""

    def __init__(self, datasource, **kwargs):
        self._datasource = datasource

    @abstractmethod
    def extract(self, force=False):
        pass

# -----------------------------------------------------------------------------#
class WebExtractor(Extractor):
    """Extracts data via scraping"""

    def __init__(self, datasource):
        super(WebExtractor, self).__init__(datasource)        

    def _extractfile(self, object):
        link = self._datasource.download_url + object
        print("Downloading data from {url}".format(url=link), end=" ")
        results = urlopen(link)
        zipfile = ZipFile(BytesIO(results.read()))
        zipfile.extractall(self._datasource.download_dir)
        print("...complete!")        


    def extract(self, force=False):
        if (os.path.exists(self._datasource.download_dir)) & (force is False):
            print("Directory exists. To overwrite, set force=True")
            return
        
        if not os.path.exists(self._datasource.download_dir):
            os.makedirs(self._datasource.download_dir)

        page = requests.get(self._datasource.url)
        soup = BeautifulSoup(page.content, 'html.parser')          
        object = eval(self._datasource.config['find']) 
        
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

    def extract(self, force=False):

        if (os.path.exists(self._datasource.download_dir)) & (force is False):
            return
        
        if not os.path.exists(self._datasource.download_dir):
            os.makedirs(self._datasource.download_dir)
        
        for key, filename in self._datasource.filenames.items():
            url = self._datasource.download_url + filename
            print("Downloading data from {url}".format(url=url), end=" ")
            results = requests.get(url)
            zipfile = ZipFile(BytesIO(results.content))
            zipfile.extractall(self._datasource.download_dir)
            print("...complete!")
        print("File download complete.")




