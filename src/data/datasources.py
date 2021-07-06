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
# Modified : Monday, July 5th 2021, 9:36:20 pm                                 #
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

from configs.config import Config, AACTConfig, DrugsConfig, LabelsConfig
from src.logging import exception_handler, logging_decorator, Logger
# -----------------------------------------------------------------------------#
# Module level logger
logger = Logger(__name__).get_logger()

class DataSource(ABC):
    """Defines a data source and methods for obtaining and staging the data.

    Parameters
    ----------
    configuration : class
        Class containing the configuration for the data source.
    """    
    def __init__(self, configuration, **kwargs):
        self._configuration = configuration
        self.name = configuration.name

    def _expired(self):
        """ Returns true if dataset has expired.

        Returns True if the difference between date_downloaded and today is 
        greater than the lifecycle from the configuration file.
        """   
        return  datetime.now() > self._configuration.download_date + \
            timedelta(days=self._configuration.lifecycle)                        
    
    def _check_url(self, url):
        """Checks validity of URL.
        Returns response if response.ok is true, otherwise writes http status
        code to the log, and returns false.
        """
        response = requests.get(url)
        if not response.ok:
            logger.warn("HTTP Status code of {code} returned for {url}"\
                .format(code=response.status_code, url=url))
        return response

    def _download(self, response):      
        """Downloads the resource once a valid url response is obtained."""
        zipfile = ZipFile(BytesIO(response.content))
        zipfile.extractall(self._configuration.persistence)    
        today = datetime.now().strftime("%Y%m%d") 
        self._configuration.download_date = today      

    @abstractmethod
    def _stage(self):    
        """ Stages data for downstream analysis and processing.

        Reads the downloaded data, then stores the data in csv format in the 
        staging area.
        """
        pass


    @abstractmethod
    def get(self):
        """Method exposed to obtain the data from its source, to store
        it in csv format in the staging area."""
        pass
    

# -----------------------------------------------------------------------------#    
class AACTDataSource(DataSource):
    """PostgreSQL Datasource"""
    __doc__ += DataSource.__doc__

    def __init__(self, configuration, dbdao):
        super(AACTDataSource, self).__init__(configuration)
        self._dbdao = dbdao
       
    def _get_url(self):
        """Formats URL."""

        url = self._configuration.url       
        url = url.format(date=datetime.now().strftime("%Y%m%d"))
        return url


    def _stage(self):
        """Extracts data from each table and stores in .csv staging area."""
        tables = self._dbdao.tables
        for table in tables:
            df = self._dbdao.read_table(table)
            filepath = self._configuration.staging + table + '.csv'
            df.to_csv(filepath)

    def _refresh_data():
        if self._expired():
            url = self._get_url()
            response = self._check_url(url)
            if response.ok:
                self._download(response)
            return response.ok

    @exception_handler
    @logging_decorator
    def get(self):
        data_refreshed = self._refresh_data()
        staged_dir = self._configuration.staging
        if data_

        
            
            
# -----------------------------------------------------------------------------#    
class DrugsDataSource(DataSource):
    """Obtains data from Drugs@FDA site"""     
    __doc__ += DataSource.__doc__    

    def __init__(self, configuration):
        super(Drugs, self).__init__(configuration)

    def _get_url(self):
        return self._configuration.webpage                 

    @exception_handler
    @logging_decorator
    def get(self):
        """Obtains the data from the Drugs@FDA site.

        Getting the URL requires some scraping so we first attempt to access
        the webpage. If we are successful, we obtain a Beautiful Soup 
        representation of the webpage which we will use to further search
        for the target resource."""
        if self._expired():
            url = self._get_url()
            response = self._check_url(url)
            # A False value for response indicates a problem accessing the site.
            # In this case, we log return status code  and gracefully end.
            if response is not False:
                soup = BeautifulSoup(response.content, 'html.parser')          
                filename = eval(configuration.find) 
                # Form the url and check accessibility.
                url = configuration.url + filename 
                response = self._check_url(url)
                if response is not False:           
                    self._download(response)
                    self._stage()

# -----------------------------------------------------------------------------#
class LabelsDataSource(DataSource):
    """Drug label information from Labels."""
    __doc__ += DataSource.__doc__

    def __init__(self, configuration):
        super(Labels, self).__init__(configuration)
        # Labels maintains a json file with all downloadable links. 
        self._url_links = configuration.links        
        response = urlopen(self._url_links)
        self._links = json.loads(response.read())                

    def _get_urls(self):        
        urls = []        
        partitions = self._url_links['results']['drug']['label']['partitions']
        for partition in partitions:
            urls.append(partition['file'])
        return urls

    def _download_urls(self, urls):
        for url in urls:
            response = self._check_url(url)
            if response is not False:
                self._download(response)        

    @exception_handler
    @logging_decorator
    def get(self):
        if self._expired():
            urls = self._get_urls()
            self._download_urls(urls)



