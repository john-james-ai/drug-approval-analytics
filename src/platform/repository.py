#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# =========================================================================== #
# Project  : Drug Approval Analytics                                          #
# Version  : 0.1.0                                                            #
# File     : \src\platform\repository.py                                      #
# Language : Python 3.9.5                                                     #
# --------------------------------------------------------------------------  #
# Author   : John James                                                       #
# Company  : nov8.ai                                                          #
# Email    : john.james@nov8.ai                                               #
# URL      : https://github.com/john-james-sf/drug-approval-analytics         #
# --------------------------------------------------------------------------  #
# Created  : Wednesday, July 21st 2021, 1:25:36 pm                            #
# Modified : Wednesday, July 21st 2021, 6:04:55 pm                            #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
"""Repository for pipeline events

This module defines the repostory for pipeline events and consists of:

    - Elements: Repository containing pipeline elements
    - Steps: Repository containing pipeline steps
    - Pipelines: Repository containing pipeline information
    - Events: Repository and unit of work for pipeline events.

Dependencies:
    Element: Class defining pipeline inputs and outputs
    Step: Class defining pipeline tasks
    Pipeline: Class defining the pipeline engine
    PipelineDatabaseAccessObject: Access object for the database.

"""
from abc import ABC, abstractmethod

from .pipeline import Element, Step, Pipeline, Event
from .database import PipelineDatabaseAccessObject
# --------------------------------------------------------------------------- #


class Repository(ABC):
    """Base class for repositories."""

    table = None

    def __init__(self, database):
        self._database = database

    @abstractmethod
    def add(self, *args, **kwargs) -> None:
        pass

    @abstractmethod
    def get(self, *args, **kwargs) -> None:
        pass

    @abstractmethod
    def get_all(self) -> None:
        pass

    @abstractmethod
    def update(self, *args, **kwargs) -> None:
        pass

    @abstractmethod
    def delete(self, *args, **kwargs) -> None:
        pass

    @abstractmethod
    def delete_all(self) -> None:
        pass
# --------------------------------------------------------------------------- #


class Elements(Repository):
    """Repository for Pipeline Element objects.

    Arguments:
        database: PipelineDatabaseAccessObject

    """

    table = 'elements'

    def __init__(self, database):
        super(Elements, self).__init__(database)

    def add(self, element: Element) -> None:
        self._database.add(Elements.table, element)

    def get(self, name: str) -> Element:
        return self._database.get(Elements.table, name)

    def get_all(self) -> list:
        return self._database.get_all('element')

    def update(self, element: Element) -> None:
        self._database.update(Elements.table, element)

    def delete(self, name: str) -> None:
        self._database.delete(Elements.table, name)

    def delete_all(self) -> None:
        self._database.delete_all(Elements.table)
# --------------------------------------------------------------------------- #


class Steps(Repository):
    """Repository for Pipeline Step objects.

    Arguments:
        database: PipelineDatabaseAccessObject

    """

    table = 'steps'

    def __init__(self, database):
        super(Steps, self).__init__(database)

    def add(self, step: Step) -> None:
        self._database.add(Steps.table, step)

    def get(self, name: str) -> Step:
        return self._database.get(Steps.table, name)

    def get_all(self) -> list:
        return self._database.get_all(Steps.table)

    def update(self, step: Step) -> None:
        self._database.update(Steps.table, step)

    def delete(self, name: str) -> None:
        self._database.delete(Steps.table, name)

    def delete_all(self) -> None:
        self._database.delete_all(Steps.table)


# --------------------------------------------------------------------------- #
class Pipelines(Repository):
    """Repository for Pipeline Pipeline objects.

    Arguments:
        database: PipelineDatabaseAccessObject

    """

    table = 'pipelines'

    def __init__(self, database):
        super(Pipelines, self).__init__(database)

    def add(self, pipeline: Pipeline) -> None:
        self._database.add(Pipelines.table, pipeline)

    def get(self, name: str) -> Pipeline:
        return self._database.get(Pipelines.table, name)

    def get_all(self) -> list:
        return self._database.get_all(Pipelines.table)

    def update(self, pipeline: Pipeline) -> None:
        self._database.update(Pipelines.table, pipeline)

    def delete(self, name: str) -> None:
        self._database.delete(Pipelines.table, name)

    def delete_all(self) -> None:
        self._database.delete_all(Pipelines.table)
# --------------------------------------------------------------------------- #


class Events(Repository):
    """Repository for Pipeline Event objects.

    Arguments:
        database: PipelineDatabaseAccessObject

    """

    table = 'events'

    def __init__(self, database):
        super(Events, self).__init__(database)

    def add(self, event: Event) -> None:
        self._database.add(Events.table, event)

    def get(self, name: str) -> Event:
        return self._database.get(Events.table, name)

    def get_all(self) -> list:
        return self._database.get_all(Events.table)

    def update(self, event: Event) -> None:
        self._database.update(Events.table, event)

    def delete(self, name: str) -> None:
        self._database.delete(Events.table, name)

    def delete_all(self) -> None:
        self._database.delete_all(Events.table)


# --------------------------------------------------------------------------- #
class Repositories:
    """Encapsulates all repository classes.

    Dependency:
        database: PipelineDatabaseAccessObject

    """

    def __init__(self):
        database = PipelineDatabaseAccessObject()
        self._elements = Elements(database)
        self._steps = Steps(database)
        self._pipelines = Pipelines(database)
        self._events = Events(database)

    @property
    def elements(self):
        return self._elements

    @property
    def steps(self):
        return self._steps

    @property
    def pipelines(self):
        return self._pipelines

    @property
    def events(self):
        return self._events
