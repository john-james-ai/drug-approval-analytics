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
# Modified : Monday, July 12th 2021, 2:05:30 am                                #
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
import unicodedata
import re
import json
from zipfile import ZipFile

import pandas as pd
from bs4 import BeautifulSoup
from bs4.element import ResultSet
from sqlalchemy import String, Integer, Column, Boolean, DateTime
from sqlalchemy.schema import ForeignKey
from sqlalchemy.dialects.postgresql import JSON, ARRAY
from sqlalchemy.ext.hybrid import hybrid_property

from src.logging import exception_handler, logging_decorator, Logger
from src.utils.dates import Parser
from src.database import Base
# -----------------------------------------------------------------------------#
class DataSource(Base):
    """Standard object that represents data source objects.

    Arguments
    ----------
        configuration (dict): Dictionary with name, webpage, baseurl, and
            other configuration items for the datasource.

    Attributes        
        name (str): Name of object. 
        webpage (str): Webpage containing the data
        baseurl (str): The baseurl for target url(s)
        urls (str): List of links to target resources.
        valid_url_exists (bool): False if no valid URL exists for a source.
        extract_dir (str): The directory to which the data will be extracted.
        staging_dir (str): The directory to which the data will be staged.
        lifecycle (int): The number of days between data refresh at the source             
        last_updated (DateTime): The date the source links were last updated
        last_extracted (DateTime): The date the source was last extracted
        last_staged (DateTime): The date the source was last staged.
        last_executed (DateTime): The date the extraction was executed, whether date
            were available for download or not.
        created (DateTime): The datetime this object was created
        created (DateTime): The datetime this object was last updated.
    """    
    __tablename__ = 'datasource'

    id = Column(Integer, primary_key=True)
    _name = Column('name', String(20))
    _webpage = Column('webpage', String(80))    
    _baseurl = Column('baseurl', String(120))
    _urls = Column('urls', ARRAY(String))
    _valid_url_exists = Column('valid_url_exists', Boolean)
    _extract_dir = Column('extract_dir', String(50))
    _staging_dir = Column('staging_dir', String(50))
    _lifecycle = Column('lifecycle', Integer)
    _last_updated = Column('last_updated', DateTime)                
    _last_extracted = Column('last_extracted', DateTime)     
    _last_staged = Column('last_staged', DateTime)         
    _last_executed =  Column('last_executed', DateTime)     
    _created = Column('created', DateTime) 
    _updated = Column('updated', DateTime) 
    
    # Supports Polymorphism
    type = Column(String)  

    __mapper_args__ = {
        'polymorphic_identity': 'datasource',
        'polymorphic_on': type
    }

    def __init__(self, configuration):
        self._name = configuration.name
        self._webpage = configuration.webpage
        self._baseurl = configuration.baseurl
        self._extract_dir = configuration.extract_dir
        self._staging_dir = configuration.staging_dir
        self._lifecycle = configuration.lifecycle
        self._last_updated = datetime(1970,1,1)
        self._last_extracted = datetime(1970,1,1)
        self._last_staged = datetime(1970,1,1)
        self._last_executed = datetime(1970,1,1)

        self._created = datetime.now()
        self._updated = datetime.now()

        self._urls = []
        self._valid_url_exists = True

    def __str__(self):
        return "{classname}({name},{webpage},{baseurl},{lifecycle})".format(
            classname=self.__class__.__name__,
            name=str(self.name),
            webpage=str(self._webpage),
            baseurl=str(self._baseurl),
            lifecycle=str(self._lifecycle)
        )

    def __repr__(self):
        return "{classname}({name},{webpage},{baseurl},{lifecycle})".format(
            classname=self.__class__.__name__,
            name=repr(self.name),
            webpage=repr(self._webpage),
            baseurl=repr(self._baseurl),
            lifecycle=repr(self._lifecycle)
        )

    # ----------------------------------------------------------------------- #
    #                     PUBLIC METHODS (AS PROPERTIES)                      #
    # ----------------------------------------------------------------------- #    
    def update_data_source_information(self):
        pass
        

    @hybrid_property
    def is_refreshable(self):
        """Indicates whether a source is eligible for updating.

        Returns True if the local copy of the source has expired and the 
        source has been updated since it was last extracted.
        """
        
        return self.is_expired and self._valid_url_exists and \
            (self._last_updated > self._last_extracted)

    @hybrid_property
    def is_expired(self):
        """ Returns True if dataset has outlived its lifecycle.

        Returns True if the difference between date_downloaded and today is 
        greater than the lifecycle.
        """   
        return  datetime.now() > (self._last_extracted + \
            timedelta(days=self._lifecycle))     

    # ----------------------------------------------------------------------- #
    #                              PROPERTIES                                 #
    # ----------------------------------------------------------------------- #
    @hybrid_property
    def name(self):
        return self._name

    @hybrid_property
    def urls(self):
        return self._urls

    @hybrid_property
    def extract_dir(self):
        return self._extract_dir

    @hybrid_property
    def staging_dir(self):
        return self._staging_dir        

    @hybrid_property
    def lifecycle(self):
        return self._lifecycle

    @hybrid_property
    def last_updated(self):
        return self._last_updated

    @last_updated.setter
    def last_updated(self, last_updated):
        self._last_updated = last_updated

    @hybrid_property
    def last_extracted(self):
        return self._last_extracted

    @last_extracted.setter
    def last_extracted(self, last_extracted):
        self._last_extracted = last_extracted
        
    @hybrid_property
    def last_staged(self):
        return self._last_staged

    @last_extracted.setter
    def last_staged(self, last_staged):
        self._last_staged = last_staged

    @hybrid_property
    def last_executed(self):
        return self._last_executed

    @last_extracted.setter
    def last_executed(self, last_executed):
        self._last_executed = last_executed        
                
    @hybrid_property
    def created(self):
        return self._created

    @hybrid_property
    def updated(self):
        return self._updated        

    

# -----------------------------------------------------------------------------#    
class AACTDataSource(DataSource):
    """Datasource for the AACT Clinical Trials Database"""
    
    __tablename__ = 'aact_datasource'

    id = Column(Integer, ForeignKey('datasource.id'), primary_key=True)   


    __mapper_args__ = {
        'polymorphic_identity': 'aact_datasource',
    } 

    def __init__(self, configuration):
        super(AACTDataSource, self).__init__(configuration)      

    # ----------------------------------------------------------------------- #
    #                           PUBLIC METHODS                                #
    # ----------------------------------------------------------------------- #    
    @exception_handler       
    def update_data_source_information(self):
        """Obtains and validates urls and timestamps associated with the data"""

        response = requests.get(self._webpage)   
        # Use BeautifulSoup to create searchable html parser
        soup = BeautifulSoup(response.content, 'html.parser')    

        # Obtain the link / url:
        # There are two main tables on the webpage: one for daily static 
        # copies and the other for monthly archives. The target url is 
        # in the first table under the heading 'Current Month's Daily Static Copies'
        table = soup.findChildren('table')[0]    
        # Grab the first row
        row = table.find_all('tr')[0]                
        # Get the link from the href element
        link = row.find('a', href=True)['href']
        # Confirm that link is from the daily static table.
        self._valid_url_exists = 'daily' in link


        # If the link is valid, format the full url and obtain its timestamp
        # and update the member variables.
        if self._valid_url_exists:
            url = self._baseurl + link
            self._urls = []
            self._urls.append(url)
            # Get the date the url was created from the same row.
            date_string = row.find_all('td')[1].text
            # Convert the text to datetime object
            self._last_updated = datetime.strptime(date_string, "%m/%d/%Y")

        self._updated = datetime.now()

# -----------------------------------------------------------------------------#    
class DrugsDataSource(DataSource):
    """Obtains data from Drugs@FDA site"""     
    
    __tablename__ = 'drugs_datasource'

    id = Column(Integer, ForeignKey('datasource.id'), primary_key=True)   

    __mapper_args__ = {
        'polymorphic_identity': 'drugs_datasource',
    }     

    def __init__(self, configuration):
        super(DrugsDataSource, self).__init__(configuration)           

    # ----------------------------------------------------------------------- #
    #                           PUBLIC METHODS                                #
    # ----------------------------------------------------------------------- #    
    @exception_handler       
    def update_data_source_information(self):
        """Obtains and validates urls and timestamps associated with the data"""

        

        response = requests.get(self._webpage)   
        response.raise_for_status()
        # Use BeautifulSoup to create searchable html parser
        soup = BeautifulSoup(response.content, 'html.parser')            
        link = soup.find(attrs={"data-entity-substitution":"media_download"})["href"]
        # Format url and add to list object.
        url = self._baseurl + link
        self._urls = []
        self._urls.append(url)

        # Find and parse the timestamp for the link.
        text = soup.find_all(string=re.compile("Data Last Updated:"))[0]
        self._last_updated = Parser().drugs_last_updated(text)           

        # Update the timestamp on this object
        self._updated = datetime.now()            


                    

# -----------------------------------------------------------------------------#
class LabelsDataSource(DataSource):
    """Drug label information from Labels."""
    
    __tablename__ = 'labels_datasource'

    id = Column(Integer, ForeignKey('datasource.id'), primary_key=True)   
    _download_links = Column('download_links', String)

    __mapper_args__ = {
        'polymorphic_identity': 'labels_datasource',
    }         

    def __init__(self, configuration):
        super(LabelsDataSource, self).__init__(configuration)    
        # Labels maintains a json file with all downloadable links. 
        self._download_links = configuration.download_links           
        self.update_data_source_information()
        
    # ----------------------------------------------------------------------- #
    #                           PUBLIC METHODS                                #
    # ----------------------------------------------------------------------- #    
    @exception_handler       
    def update_data_source_information(self):
        """Obtains and validates urls and timestamps associated with the data"""        

        # First, obtain the json containing all download links for the site.        
        response = urlopen(self._download_links)
        links = json.loads(response.read())     

        # Extract all the relevant links. The data are split into 10
        # partitions, each with its own link.      
        partitions = links['results']['drug']['label']['partitions']
        self._urls = []
        for partition in partitions:
            self._urls.append(partition['file'])
        
        # Extract the timestamp for those links.
        last_updated_text = links['results']['drug']['label']['export_date']
        self._last_updated = datetime.strptime(last_updated_text, "%Y-%m-%d")
        
        self._updated = datetime.now()




