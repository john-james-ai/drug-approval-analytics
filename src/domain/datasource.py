#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# =========================================================================== #
# Project  : Drug Approval Analytics                                          #
# Version  : 0.1.0                                                            #
# File     : \src\domain\datasource.py                                        #
# Language : Python 3.9.5                                                     #
# --------------------------------------------------------------------------  #
# Author   : John James                                                       #
# Company  : nov8.ai                                                          #
# Email    : john.james@nov8.ai                                               #
# URL      : https://github.com/john-james-sf/drug-approval-analytics         #
# --------------------------------------------------------------------------  #
# Created  : Sunday, August 15th 2021, 9:31:23 am                             #
# Modified : Sunday, August 15th 2021, 11:52:54 am                            #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
"""Defines a DataSource entity."""
from abc import ABC, abstractmethod

# --------------------------------------------------------------------------- #
#                           DATASOURCE                                        #
# --------------------------------------------------------------------------- #


class DataSource(ABC):

    def __init__(self, name: str, source_type: str, webpage: str, link: str,
                 link_type: str, extractor: Extractor, frequency: int = 1,
                 lifecycle: int = 7, creator: int = None,
                 has_changed: bool = False,
                 source_updated: datetime = None, title: str = None,
                 description: str = None, coverage: str = None,
                 maintainer: str = None) -> None:

        self._name = name
        self._type = type
        self._description = description
        self._source_type = source_type
        self._webpage = webpage
        self._link = link
        self._link_type = link_type
        self._frequency = frequency
        self._lifecyele = lifecycle
        self._has_changed = has_changed
        self._source_updated = source_updated
        self._created = datetime.now()
        self._modified = None
        self._extracted = None
        self._next_extract = None

    @property
    def frequency(self) -> bool:
        return self._frequency

    @frequency.setter
    def frequency(self, frequency: bool) -> None:
        self._frequency = frequency

    @property
    def lifecycle(self) -> bool:
        return self._lifecycle

    @lifecycle.setter
    def lifecycle(self, lifecycle: bool) -> None:
        self._lifecycle = lifecycle

    @property
    def has_changed(self) -> bool:
        return self._has_changed

    @has_changed.setter
    def has_changed(self, has_changed: bool) -> None:
        self._has_changed = has_changed

    @property
    def extracted(self) -> datetime:
        return self._extracted

    @extracted.setter
    def extracted(self, extracted: datetime) -> None:
        self._extracted = extracted

    @property
    def source_updated(self) -> bool:
        return self._source_updated

    @source_updated.setter
    def source_updated(self, source_updated: datetime) -> None:
        self._source_updated = source_updated

    @property
    def next_extract(self) -> datetime:
        return self._next_extract

    @next_extract.setter
    def next_extract(self, next_extract: datetime) -> None:
        self._next_extract = next_extract

# --------------------------------------------------------------------------- #
#                              VISITOR                                        #
# --------------------------------------------------------------------------- #


class Visitor(ABC):
    """Base class for data source visitor subclasses.

    Visitors are responsible for visiting the data source websites and
    engaging the appropriate extractor to download the data to the 
    designated directory.

    Arguments:
        webpage (str): The webpage for the datasource
        link (str): A secondary link used to locate the uri

    Attributes:
        has_changed (bool): Whether the datasource has changed
            since last extracted.
        downloadable (bool): Indicates whether the dataset is 
            downloadable. This means that the dataset has 
            been updated since the last extract date AND 
            the last extract date was greater or equal 
            to the lifecycle. 
        uris (list): The list of uris for the source

    """

    def __init__(self, datasource: DataSource) -> list:
        self._datasource = datasource
        self._updated = None
        self._has_changed = False
        self._downloadable = False
        self._uris = None

    @abstractmethod
    def _execute(self, metadata: dict) -> dict:
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

    def execute(self) -> None:
        self._started = datetime.now()
        self._uris = self._execute()
        self._ended = datetime.now()

    @property
    def uris(self) -> list:
        return self._uris

    @property
    def start(self) -> datetime:
        return self._start

    @property
    def end(self) -> datetime:
        return self._end

    @property
    def datasource(self) -> DataSource:
        return self._datasource

# --------------------------------------------------------------------------- #
#                                STUDIES                                      #
# --------------------------------------------------------------------------- #


class Studies(Visitor):
    """Extracts and updates AACT data source uri and currency information."""

    _source_name = 'studies'

    def __init__(self, datasource: DataSource) -> list:
        super(Studies, self).__init__(datasource)

    def _execute(self) -> list:

        webpage = self._datasource.webpage
        baseuri = self._datasource.link

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
        uri = row.find('a', href=True)['href']
        # Confirm that link is from the daily static table.
        valid_uri_exists = 'daily' in uri

        # If the link is valid, format the full uri and obtain its timestamp
        # and update the member variables.
        if valid_uri_exists:
            uri = baseuri + uri
            uris = []
            uris.append(uri)
            # Get the date the uri was created from the same row.
            date_string = row.find_all('td')[1].text
            # Convert the text to datetime object
            self._datasource.source_updated = datetime.strptime(
                date_string, "%m/%d/%Y")
            if self._source_updated > self._datasource.extracted:
                self._datasource.has_changed = True:

            if self._datasource.has_changed and self._datasource.next_extract not > datetime.now().date():
                self._datasource.downloadable = True
            else:
                self._datasource.downloadable = False
        else:
            self._datasource.downloadable = False
        return urls
