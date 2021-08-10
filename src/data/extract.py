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
# Modified : Monday, August 9th 2021, 8:20:11 pm                              #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
"""Extract Module

Classes responsible for extracting data from designated web sources.

"""
from abc import ABC, abstractmethod
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


# -----------------------------------------------------------------------------#
#                               HABITUE                                        #
# -----------------------------------------------------------------------------#
class Habitue(ABC):
    """Base class for data source visitor subclasses.

    Classes frequently visit data sources to obtain current links, determine
    if the data has been updated, and whether it is time to download
    fresh data from a data source. 

    Arguments:
        name (str): The registered name for the data source.
        database (DBDao): Database access object containing the meta data

    Attributes:
        expired (bool): Indicates if a data source has not been updated
            in the last 'lifecycle' days.
        updated (bool): Indicates whether a data source has been
            updated since last extracted.
        extract (bool): True if a data source has not been extracted
            in 'lifecycle' days AND the data source has been updated
            since it was last extracted; False otherwise.
        uris (list): List of URIs for the data source.
        num_uris: The number of individual URIs to extract.        

    """

    def __init__(self, name, database):
        self._name = name
        self._database = database
        self._uris = []
        self._num_uris = 0

    @abstractmethod
    def __str__(self):
        pass

    @abstractmethod
    def __repr__(self):
        pass

    def get_metadata(self):
        self._database.get()

    @abstractmethod
    def visit(self):
        pass

    @property
    def uris(self):
        return self._uris

    @property
    def num_uris(self):
        return self._num_uris
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
        return datetime.now() > (self._last_extracted +
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
        link = soup.find(
            attrs={"data-entity-substitution": "media_download"})["href"]
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
