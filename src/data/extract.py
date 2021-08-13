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
# Modified : Thursday, August 12th 2021, 5:52:54 am                           #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
"""Extract Module

Classes responsible for extracting data from designated web sources.

"""
from abc import ABC, abstractmethod
from datetime import datetime
import logging
import requests
# from urilib.request import uriopen
# from io import BytesIO
# import unicodedata
# import re
# import json
# from zipfile import ZipFile

from bs4 import BeautifulSoup
# from bs4.element import ResultSet
import pandas as pd

from ..platform.metadata.repository import DataSource, DataSourceEvent
from src.platform.config import DBCredentials
# --------------------------------------------------------------------------- #
logger = logging.getLogger(__name__)
# --------------------------------------------------------------------------- #
#                               HABITUE                                       #
# --------------------------------------------------------------------------- #


class Habitue(ABC):
    """Base class for data source visitor subclasses.

    Classes frequently visit data sources to obtain current links, determine
    if the data have been updated, and notes whether it is time to download
    fresh data from a data source in the metadata repository.

    Arguments:
        name (str): The registered name for the data source.
        credentials (DBCredentials): Database credentials

    """

    def __init__(self, credentials: DBCredentials):
        self._name = None
        self._repository = DataSource(credentials=credentials)
        self._events = DataSourceEvent(credentials=credentials)
        self._started = None
        self._ended = None

    @abstractmethod
    def _get_metadata_from_source(self, metadata: dict) -> dict:
        """Extracts source metadata from the source webpage.

        Provides information about the currency of the source.
        Arguments:
            metadata (pd.DataFrame): Data source metadata obtained
                from the repository.
        Returns:
            source (dict): A dictionary containing:
                uris (list): One or more current URIs
                source_updated: Date the source was last updated
        """
        pass

    def _get_metadata_from_repo(self) -> pd.DataFrame:
        """Reads source metadata from repository."""

        metadata = self._repository.read(name=self._name).to_dict('records')
        return metadata[0]

    def _update_repo(self, repo: pd.DataFrame, source: dict) -> None:
        """Updates repository with current state of source."""

        name = self._name
        version = repo['version']
        uris = source['uris']
        has_changed = repo['has_changed']
        source_updated = source['source_updated']
        updated = datetime.now()
        updated_by = self.__class__.__name__

        if not repo['has_changed'] and\
           (repo['source_updated'] != source['source_updated'].date()):
            has_changed = True
            version += 1

        self._repository.update(name=name,
                                version=version,
                                uris=uris,
                                has_changed=has_changed,
                                source_updated=source_updated,
                                updated=updated,
                                updated_by=updated_by)

    def _update_events(self, repo: pd.DataFrame) -> None:
        self._events.create(name="visit",
                            datasource_id=repo['id'],
                            started=self._started,
                            ended=self._ended,
                            created=datetime.now(),
                            created_by=self.__class__.__name__,
                            return_code=0,
                            return_value='Success'
                            )

    def execute(self) -> None:
        repo = self._get_metadata_from_repo()
        self._started = datetime.now()
        source = self._get_metadata_from_source(repo)
        self._ended = datetime.now()
        self._update_repo(repo, source)
        self._update_events(repo)


# --------------------------------------------------------------------------- #
#                                AACT                                         #
# --------------------------------------------------------------------------- #
class Studies(Habitue):
    """Extracts and updates AACT data source uri and currency information."""

    _source_name = 'studies'

    def __init__(self, credentials: DBCredentials):
        super(Studies, self).__init__(credentials=credentials)
        self._name = Studies._source_name

    def _get_metadata_from_source(self, metadata: dict) -> dict:
        source = {}
        webpage = metadata['webpage']
        baseuri = metadata['link']

        response = requests.get(webpage)
        # Use BeautifulSoup to create searchable html parser
        soup = BeautifulSoup(response.content, 'html.parser')

        # Obtain the link / uri:
        # There are two main tables on the webpage: one for daily static
        # copies and the other for monthly archives. The target uri is
        # in the first table under the heading 'Current Month's
        # Daily Static Copies'
        table = soup.findChildren('table')[0]
        # Grab the first row
        row = table.find_all('tr')[0]
        # Get the link from the href element
        link = row.find('a', href=True)['href']
        # Confirm that link is from the daily static table.
        valid_uri_exists = 'daily' in link

        # If the link is valid, format the full uri and obtain its timestamp
        # and update the member variables.
        if valid_uri_exists:
            uri = baseuri + link
            uris = []
            uris.append(uri)
            # Get the date the uri was created from the same row.
            date_string = row.find_all('td')[1].text
            # Convert the text to datetime object
            source_updated = datetime.strptime(date_string, "%m/%d/%Y")
        else:
            uris = metadata['uris']
            source_updated = metadata['source_updated']

        source['uris'] = uris
        source['source_updated'] = source_updated
        return source
