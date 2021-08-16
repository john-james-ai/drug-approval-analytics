#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# =========================================================================== #
# Project  : Drug Approval Analytics                                          #
# Version  : 0.1.0                                                            #
# File     : \src\operations\artifacts.py                                     #
# Language : Python 3.9.5                                                     #
# --------------------------------------------------------------------------  #
# Author   : John James                                                       #
# Company  : nov8.ai                                                          #
# Email    : john.james@nov8.ai                                               #
# URL      : https://github.com/john-james-sf/drug-approval-analytics         #
# --------------------------------------------------------------------------  #
# Created  : Saturday, July 31st 2021, 3:44:38 am                             #
# Modified : Monday, August 16th 2021, 12:50:46 am                            #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
# %%
"""Repository of Metadata."""
from abc import ABC, abstractmethod
from datetime import datetime
import logging
from types import Union
import pandas as pd

from ..platform.database.connect import PGConnectionPool
from ..platform.database.context import PGDao
from ..platform.database.connect import ConnectionFactory
from ..platformsrc.application.config import rx2m_login, DBCredentials
# --------------------------------------------------------------------------- #
logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
#                              REPOSITORY                                     #
# --------------------------------------------------------------------------- #
class Repository(ABC):

    def __init__(self, context; Context) -> None:
        self._context = context
        self._data = None
        self._index = 0

    @abstractmethod
    def __next__(self):

    @abstractmethod
    def get(self, name: str, *args, **kwargs) -> pd.DataFrame:
        pass

    @abstractmethod
    def add(self, entity: Entity) -> None:
        pass

    @abstractmethod
    def update(self, entity: Entity) -> None:
        pass

    @abstractmethod
    def remove(self, name: str, *args, **kwargs) -> None:
        pass


# --------------------------------------------------------------------------- #


class DataSource(Artifact):

    def __init__(self, credentials: DBCredentials,
                 autocommit: bool = False) -> None:
        super(DataSource, self).__init__(credentials=credentials,
                                         autocommit=autocommit)
        self._table = 'datasource'

    def create(self,
               name: str,
               source_type: str,
               version: int,
               webpage: str,
               link: str,
               link_type: str,
               frequency: int,
               lifecycle: int,
               creator: int,
               has_changed: bool,
               source_updated: datetime,
               created: datetime,
               created_by: str,
               title: str = None,
               description: str = None,
               coverage: str = None,
               maintainer: str = None,
               **kwargs) -> None:

        columns = ["name", "source_type", "version", "webpage",
                   "link", "link_type", "frequency", "lifecycle", "creator",
                   "has_changed", "source_updated", "created", "created_by",
                   "title", "description", "coverage", "maintainer"]

        values = [name, source_type, version, webpage,
                  link, link_type, frequency, lifecycle, creator,
                  has_changed, source_updated, created, created_by,
                  title, description, coverage, maintainer]

        for k, v in kwargs.items():
            columns.append(k)
            values.append(v)
        self._dao.create(table=self._table, columns=columns, values=values,
                         schema=self._schema)

    def read(self, name: str = None) -> pd.DataFrame:
        if name is not None:
            result = self._dao.read(table=self._table,
                                    filter_key='name',
                                    filter_value=name,
                                    schema=self._schema)
        else:
            result = self._dao.read(table=self._table,
                                    schema=self._schema)
        return result

    def update(self, name: str, version: int, uris: list,
               has_changed: bool, source_updated: datetime,
               updated: datetime, updated_by: str) -> None:

        self._dao.connect()
        self._dao.begin()
        self._dao.update(table=self._table, column='uris', value=uris,
                         filter_key='name', filter_value=name,
                         schema=self._schema)
        self._dao.update(table=self._table, column='has_changed',
                         value=has_changed,
                         filter_key='name', filter_value=name,
                         schema=self._schema)
        self._dao.update(table=self._table, column='source_updated',
                         value=source_updated,
                         filter_key='name', filter_value=name,
                         schema=self._schema)
        self._dao.update(table=self._table, column='updated',
                         value=updated,
                         filter_key='name', filter_value=name,
                         schema=self._schema)
        self._dao.update(table=self._table, column='updated_by',
                         value=updated_by,
                         filter_key='name', filter_value=name,
                         schema=self._schema)
        self._dao.commit()
        self._dao.close()

    def delete(self, name) -> None:
        self._dao.delete(table=self._table, filter_key='name', filter_value=name,
                         schema=self._schema)


# --------------------------------------------------------------------------- #
#                              EVENTS                                         #
# --------------------------------------------------------------------------- #
class Event(ABC):

    def __init__(self, credentials: DBCredentials, autocommit=True,
                 *args, **kwargs) -> None:
        self._credentials = credentials
        self._dao = PGDao(credentials=credentials, autocommit=autocommit)
        self._schema = 'public'

    @abstractmethod
    def add(self, name: str, *args, **kwargs) -> None:
        pass

    @abstractmethod
    def read(self, name: str = None, *args, **kwargs) -> \
            pd.DataFrame:
        pass

    @abstractmethod
    def delete(self, id: int, *args, **kwargs) -> None:
        pass


# --------------------------------------------------------------------------- #
class DataSourceEvent(Event):

    def __init__(self, credentials: DBCredentials) -> None:
        super(DataSourceEvent, self).__init__(credentials=credentials)
        self._table = 'datasourceevent'

    def create(self, **kwargs) -> None:
        columns = [k for k in kwargs.keys()]
        values = [v for v in kwargs.values()]

        self._dao.create(table=self._table, columns=columns, values=values,
                         schema=self._schema)

    def read(self, name: str = None) -> pd.DataFrame:
        if name is None:
            df = self._dao.read(table=self._table)
        else:
            df = self._dao.read(table=self._table,
                                filter_key="name", filter_value=name)
        return df

    def delete(self, id: int = None) -> pd.DataFrame:
        self._dao.delete(table=self._table, filter_key="id", filter_value=id)
