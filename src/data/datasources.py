#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#==============================================================================#
# Project  : Predict-FDA                                                       #
# Version  : 0.1.0                                                             #
# File     : \sources.py                                                       #
# Language : Python 3.9.5                                                      #
# -----------------------------------------------------------------------------#
# Author   : John James                                                        #
# Company  : nov8.ai                                                           #
# Email    : john.james@nov8.ai                                                #
# URL      : https://github.com/john-james-sf/predict-fda                      #
# -----------------------------------------------------------------------------#
# Created  : Thursday, July 1st 2021, 9:54:37 pm                               #
# Modified : Sunday, July 4th 2021, 3:30:17 pm                                 #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#
from abc import ABC, abstractmethod
import os
from datetime import datetime, timedelta
import requests
from urllib.request import urlopen
from io import BytesIO
import json
from zipfile import ZipFile

from bs4 import BeautifulSoup
from bs4.element import ResultSet

from configs.config import Config
from src.logging import exception_handler, logging_decorator
# -----------------------------------------------------------------------------#

class DataSource(ABC):
    """Abstract base class for DataSource objects"""
    def __init__(self, name, config_parser):
        self.name = name
        self._config_parser = config_parser       
        self._persistence = self._config_parser.get(name, 'persistence')
        if not os.path.exists(self._persistence):
            os.makedirs(self._persistence)

    def _expired(self):
        """True if the local copy should be refreshed."""   

        download_date = int(self._config_parser.get(self.name, 'download_date'))
        lifecycle = int(self._config_parser.get(self.name, 'lifecycle'))
        if download_date == 0:
            return True
        else:
            download_date = datetime.strptime(str(download_date), "%Y%m%d")            
            return  datetime.now() > download_date + timedelta(days=lifecycle)                        
    
    @logging_decorator
    def _file_not_found(self):
        """Created for logging purposes"""
        pass

    @logging_decorator
    def _download(self, url):        
        results = urlopen(url)
        zipfile = ZipFile(BytesIO(results.read()))
        zipfile.extractall(self._persistence)    
        today = datetime.now().strftime("%Y%m%d")            
        self._config_parser.set(section=self.name, option='download_date', value=today)    

    @abstractmethod
    def get(self):
        pass


# -----------------------------------------------------------------------------#    
class PGDataSource(DataSource):
    """PostgreSQL Datasource"""
    def __init__(self, name, config_parser):
        super(PGDataSource, self).__init__(name, config_parser)
        
    @exception_handler
    @logging_decorator
    def get(self):
        if self._expired():
            url = self._config_parser.get(self.name, 'url')            
            url = url.format(date=datetime.now().strftime("%Y%m%d"))
            response = requests.get(url)
            if response.ok:
                self._download(url)
            else:
                self._file_not_found()

# -----------------------------------------------------------------------------#    
class DrugsFDA(DataSource):
    """Obtains data from Drugs@FDA site"""         
    def __init__(self, name, config_parser):
        super(DrugsFDA, self).__init__(name, config_parser)

    @logging_decorator
    def get(self):
        if self._expired():
            # Find page to search and find filename        
            webpage = self._config_parser.get(self.name, 'webpage')
            page = requests.get(webpage)
            soup = BeautifulSoup(page.content, 'html.parser')          
            filename = eval(self._config_parser.get(self.name,'find')) 
            # Concatenate url and filename 
            baseurl = self._config_parser.get(self.name, 'url')
            url = baseurl + filename
            self._download(url)

# -----------------------------------------------------------------------------#
class OpenFDA(DataSource):
    """Drug label information from OpenFDA."""
    def __init__(self, name, config_parser):
        super(OpenFDA, self).__init__(name, config_parser)
        # OpenFDA maintains a json file with all downloadable links. 
        url_links = self._config_parser.get(self.name, 'links')        
        response = urlopen(url_links)
        self._links = json.loads(response.read())                

    def _expired(self):
        export_date = self._links['results']['drug']['label']['export_date']        
        export_date = datetime.strptime(str(export_date), "%Y-%m-%d")                    
        
        download_date = self._config_parser.get(self.name, 'download_date')
        download_date = datetime.strptime(str(download_date), "%Y%m%d")            
        return export_date != download_date


    def _get_urls(self):        
        urls = []        
        partitions = self._links['results']['drug']['label']['partitions']
        for partition in partitions:
            urls.append(partition['file'])
        return urls
        
    @logging_decorator    
    def get(self):
        if self._expired():
            urls = self._get_urls()            
            for url in urls:
                self._download(url)



