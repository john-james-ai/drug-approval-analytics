#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# =========================================================================== #
# Project  : Drug Approval Analytics                                          #
# Version  : 0.1.0                                                            #
# File     : \src\data\extract.py                                             #
# Language : Python 3.9.5                                                     #
# --------------------------------------------------------------------------  #
# Author   : John James                                                       #
# Company  : nov8.ai                                                          #
# Email    : john.james@nov8.ai                                               #
# URL      : https://github.com/john-james-sf/drug-approval-analytics         #
# --------------------------------------------------------------------------  #
# Created  : Saturday, July 17th 2021, 2:00:13 pm                             #
# Modified :                                                                  #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#==============================================================================#
# Project  : Drug Approval Analytics                                           #
# Version  : 0.1.0                                                             #
# File     : \src\data\extract.py                                              #
# Language : Python 3.9.5                                                      #
# -----------------------------------------------------------------------------#
# Author   : John James                                                        #
# Company  : nov8.ai                                                           #
# Email    : john.james@nov8.ai                                                #
# URL      : https://github.com/john-james-sf/drug-approval-analytics          #
# -----------------------------------------------------------------------------#
# Created  : Saturday, July 17th 2021, 2:00:13 pm                              #
# Modified : Saturday, July 17th 2021, 5:26:31 pm                              #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#
"""Data Sources Module

This module defines the data sources used in the project. A DataSource base
class defines the interface and attributes inherited by source specific
descendent classes.  All datasources are stored in the PostgreSQL metadata
database using Object-relational mapping capabilities of SQLAlchemy.

"""
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

from ..utils.config import Config
from .database import Base
# -----------------------------------------------------------------------------#
class DataSource(Base):
    """Class defining a data source used in this project.

    Arguments
    ----------
        name (str): The name of the data source

    Attributes
        name (str): Name of object.
        title (str): The title for the source.
        description (str): A text description for reporting purposes
        creator (str): The organization responsible for producing the source.
        maintainer (str): The organization that maintains the data and its distribution
        webpage (str): Webpage containing the data
        url_type (str): The type of url, i.e. baseurl or direct to the resource
        url (str): The url value of the aforementioned type.
        metadata (str): URL to additional metadata if available.
        media_type (str): The format, or more formally, the MIME type of the data as per RFC2046
        frequency (str): The frequency by which the source is updated.
        coverage (str): The temporal range of the data if available.
        lifecycle (int): The number of days between data refresh at the source
        last_updated (DateTime): The date the source links were last updated
        last_extracted (DateTime): The date the source was last extracted
        last_staged (DateTime): The date the source was last staged

        created (DateTime): The datetime this object was created
        updated (DateTime): The datetime this object was last updated.
    """
    __tablename__ = 'datasource'

    id = Column(Integer, primary_key=True)
    _name = Column('name', String(20))
    _title = Column('name', String(120))
    _description = Column('description', String(240))
    _creator = Column('creator', String(40))
    _maintainer = Column('maintainer', String(40))
    _webpage = Column('webpage', String(80))
    _url_type = Column('url_type', String(80))
    _url = Column('url', String(120))
    _metadata =  Column('metadata', String(80))
    _media_type =  Column('media_type', String(16))
    _frequency =  Column('frequency', String(32))
    _coverage =  Column('coverage', String(40))
    _lifecycle = Column('lifecycle', Integer)
    _last_updated = Column('last_updated', DateTime)
    _last_extracted = Column('last_extracted', DateTime)
    _last_staged = Column('last_staged', DateTime)
    _created = Column('created', DateTime)
    _updated = Column('updated', DateTime)

    # Supports Polymorphism
    type = Column(String)

    __mapper_args__ = {
        'polymorphic_identity': 'datasource',
        'polymorphic_on': type
    }

    def __init__(self, name):
        parser = Config()
        config = parser.get_section(name)

        self._name = name
        self._title = config.title
        self._description = config.description
        self._creator = config.creator
        self._maintainer = config.maintainer
        self._webpage = config.webpage
        self._url = config.url
        self._url_type = config.url_type
        self._metadata = config.metadata
        self._media_type = config.media_type
        self._frequency = config.frequency
        self._coverage = config.coverage
        self._lifecycle = config.lifecycle
        self._last_updated = config.last_updated
        self._last_extracted = config.last_extracted
        self._last_staged = config.last_staged

        self._created = datetime.now()
        self._updated = datetime.now()

        self._urls = []

    def __str__(self):
        text = ""
        text += "DataSource"
        text += "----------"
        for key, value in __dict__:
            text += "{} = {}".format(key,value)
        text += "----------"
        return text

    def __repr__(self):
        return "{classname}({name})".format(
            classname=self.__class__.__name__,
            name=repr(self._name))

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
    def last_accessed(self):
        return self._last_executed

    @last_accessed.setter
    def last_accessed(self, last_executed):
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

    def __init__(self, config):
        super(AACTDataSource, self).__init__(config)

    # ----------------------------------------------------------------------- #
    #                           PUBLIC METHODS                                #
    # ----------------------------------------------------------------------- #
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

    def __init__(self, config):
        super(DrugsDataSource, self).__init__(config)

    # ----------------------------------------------------------------------- #
    #                           PUBLIC METHODS                                #
    # ----------------------------------------------------------------------- #
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

    def __init__(self, config):
        super(LabelsDataSource, self).__init__(config)
        # Labels maintains a json file with all downloadable links.
        self._download_links = config.download_links
        self.update_data_source_information()

    # ----------------------------------------------------------------------- #
    #                           PUBLIC METHODS                                #
    # ----------------------------------------------------------------------- #
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




