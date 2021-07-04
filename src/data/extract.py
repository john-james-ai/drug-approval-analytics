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
# Modified : Thursday, July 1st 2021, 11:46:49 pm                              #
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
from .agents import Agent
# -----------------------------------------------------------------------------#
class WebExtractor(Agent):
    """Extracts data via scraping"""

    def __init__(self):    
        pass

    def _extractfile(self, object):
        link = self._datasource.download_url + object
        print("Downloading data from {url}".format(url=link), end=" ")
        results = urlopen(link)
        zipfile = ZipFile(BytesIO(results.read()))
        zipfile.extractall(self._datasource.download_dir)
        print("...complete!")        


    def execute(self, datasource):
        if (os.path.exists(self._datasource.download_dir)):
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
class ZipExtractor(Agent):
    """Downloads data from static website."""

    def __init__(self):
        pass

    def execute(self, datasource):

        if (os.path.exists(datasource.download_location)):
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




