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
# Modified : Wednesday, July 7th 2021, 12:48:13 am                             #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#
import os
from datetime import datetime, timedelta
import requests
from urllib.request import urlopen
from io import BytesIO
import json
from zipfile import ZipFile

import pandas as pd
from bs4 import BeautifulSoup
from bs4.element import ResultSet
from sqlalchemy import String, Integer, Column, Boolean, DateTime
from sqlalchemy.schema import ForeignKey
from sqlalchemy.dialects.postgresql import JSON, ARRAY

from configs.config import Config, AACTConfig, DrugsConfig, LabelsConfig
from src.logging import exception_handler, logging_decorator, Logger
from src.database import Base

# -----------------------------------------------------------------------------#
# Module variables
logger = Logger(__name__).get_logger()


class DataSource(Base):
    """Standard object that represents data source objects.

    Attributes:        
        name (str): Name of object. 
        webpage (str): Webpage containing the data
        url (str): The link to the target resource
        persistence (str): Filepath where data is stored  
        lifecycle: Int the number of days between data refresh at the source             
        download_date: Datetime indicating the date the data was last downloaded      
    """    
    __tablename__ = 'datasource'

    id = Column(Integer, primary_key=True)
    name = Column('name', String)
    webpage = Column('webpage', String)    
    url = Column('url', String)
    persistence = Column('persistence', String)
    lifecycle = Column('lifecycle', Integer)
    download_date = Column('download_date', DateTime)     
    filenames = Column('filenames', ARRAY(String))
    
    # Supports Polymorphism
    type = Column(String)  

    __mapper_args__ = {
        'polymorphic_identity': 'datasource',
        'polymorphic_on': type
    }

    def __init__(self, configuration):
        self.name = configuration.name
        self.webpage = configuration.webpage
        self.url = configuration.url
        self.persistence = configuration.persistence
        self.lifecycle = configuration.lifecycle
        self.download_date = datetime.strptime("2000-01-01","%Y-%m-%d")
        self._zipinfo = {}
        self.filenames = []              

    @property    
    def files_extracted(self):
        return False if len(self.filenames) == 0 else True

    
    def _expired(self):
        """ Returns true if dataset has expired.

        Returns True if the difference between date_downloaded and today is 
        greater than the lifecycle from the configuration file.
        """   
        return  datetime.now() > self.download_date + \
            timedelta(days=self.lifecycle)          
    
    
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

    
    def _set_metadata(self, zipfile, response):
        """Provides information about the zip file members."""
        zipfilename = os.path.splitext(response.url)[0]
        self._zipinfo[zipfilename] = zipfile.infolist()        

        for filename in zipfile.namelist():
            self.filenames.append(filename)
            
        self.download_date = datetime.now().strftime("%Y%m%d")         

    
    def _download(self, response):      
        """Downloads the resource."""
        zipfile = ZipFile(BytesIO(response.content))
        zipfile.extractall(self.persistence)    
        
        self._set_metadata(zipfile, response)

    
    def get_data(self):
        """Method exposed to obtain the data from its source, to store
        it in csv format in the staging area."""
        pass
    
    def summary(self):
        # Create Full Summary
        zipname = []
        filename = []
        size = []
        compressed = []
        modified = []

        for name, infolist in self._zipinfo.items():
            for zipinfo in infolist:
                zipname.append(name)
                filename.append(zipinfo.filename)
                size.append(zipinfo.file_size)
                compressed.append(zipinfo.compress_size)
                date = str(zipinfo.date_time[0]) +\
                    "-" + str(zipinfo.date_time[1]) + \
                    "-" + str(zipinfo.date_time[2])
                modified.append(date)

        data = {"URL": zipname, "Filename": filename, "Size": size,
                "Compressed": compressed, "Modified": modified}
        df = pd.DataFrame(data)

        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', 1000)
        pd.set_option('display.colheader_justify', 'center')
        pd.set_option('display.precision', 2)        
        display(df)
        return df



    

# -----------------------------------------------------------------------------#    
class AACTDataSource(DataSource):
    """Datasource for the AACT Clincal Trials Database"""
    __doc__ += DataSource.__doc__
    __tablename__ = 'aact_datasource'

    id = Column(Integer, ForeignKey('datasource.id'), primary_key=True)   


    __mapper_args__ = {
        'polymorphic_identity': 'aact_datasource',
    } 

    def __init__(self, configuration):
        super(AACTDataSource, self).__init__(configuration)        
       
    def _get_url(self):
        """Formats URL."""

        url = self.url       
        url = url.format(date=datetime.now().strftime("%Y%m%d"))
        return url

    @exception_handler
    @logging_decorator
    def get(self):
        if self._expired():
            url = self._get_url()
            response = self._check_url(url)
            if response.ok:
                self._download(response)
            
# -----------------------------------------------------------------------------#    
class DrugsDataSource(DataSource):
    """Obtains data from Drugs@FDA site"""     
    __doc__ += DataSource.__doc__        
    __tablename__ = 'drugs_datasource'

    id = Column(Integer, ForeignKey('datasource.id'), primary_key=True)   

    __mapper_args__ = {
        'polymorphic_identity': 'drugs_datasource',
    }     

    def __init__(self, configuration):
        super(DrugsDataSource, self).__init__(configuration)    

    def _get_url(self):
        return self.webpage

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
            if response.ok:
                soup = BeautifulSoup(response.content, 'html.parser')          
                filename = soup.find(attrs={"data-entity-substitution":"media_download"})["href"]
                # Form the url and check accessibility.
                url = self.url + filename 
                response = self._check_url(url)
                if response.ok:           
                    self._download(response)
                    

# -----------------------------------------------------------------------------#
class LabelsDataSource(DataSource):
    """Drug label information from Labels."""
    __doc__ += DataSource.__doc__
    __tablename__ = 'labels_datasource'

    id = Column(Integer, ForeignKey('datasource.id'), primary_key=True)   
    download_links = Column('download_links', String)

    __mapper_args__ = {
        'polymorphic_identity': 'labels_datasource',
    }         

    def __init__(self, configuration):
        super(LabelsDataSource, self).__init__(configuration)    
        # Labels maintains a json file with all downloadable links. 
        self.download_links = configuration.download_links              

    def _check_url(self, url):
        response = requests.get(url)
        response.raise_for_status()
        return response       


    def _get_urls(self):        
        urls = []        
        response = urlopen(self.download_links)
        links = json.loads(response.read())          
        partitions = links['results']['drug']['label']['partitions']
        for partition in partitions:
            urls.append(partition['file'])
        return urls

    def _download_urls(self, urls):
        for url in urls:
            response = self._check_url(url)
            if response.ok:
                self._download(response)        

    @exception_handler
    @logging_decorator
    def get(self):
        if self._expired():
            urls = self._get_urls()
            self._download_urls(urls)



