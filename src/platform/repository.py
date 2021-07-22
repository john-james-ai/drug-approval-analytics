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
# Modified : Thursday, July 22nd 2021, 1:18:08 am                             #
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
    PipelineDatabaseAccessObject: Connection to this access object.

"""
from abc import ABC, abstractmethod

from .pipeline import Element, Step, Pipeline, Event
from .connection import PipelineDatabaseAccessObject
# --------------------------------------------------------------------------- #


class Repository(ABC):
    """Base class for all repositories..

    Arguments:
        connection: PipelineDatabaseAccessObject.connection

    """

    table = None

    def __init__(self, connection):
        self._connection = connection

    def open(self) -> None:
        self._cursor = self._connection.cursor()

    def close(self) -> None:
        self._cursor.commit()
        self._connection.close()

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
        connection: PipelineDatabaseAccessObject.connection

    """

    table = 'elements'

    def __init__(self, connection):
        super(Elements, self).__init__(connection)

    def add(self, element: Element) -> None:
        self._connection.add(Elements.table, element)

    def get(self, name: str) -> Element:
        return self._connection.get(Elements.table, name)

    def get_all(self) -> list:
        return self._connection.get_all('element')

    def update(self, element: Element) -> None:
        self._connection.update(Elements.table, element)

    def delete(self, name: str) -> None:
        self._connection.delete(Elements.table, name)

    def delete_all(self) -> None:
        self._connection.delete_all(Elements.table)
# --------------------------------------------------------------------------- #


class Steps(Repository):
    """Repository for Pipeline Step objects.

    Arguments:
        connection: PipelineDatabaseAccessObject.connection

    """

    table = 'steps'

    def __init__(self, connection):
        super(Steps, self).__init__(connection)

    def add(self, step: Step) -> None:
        self._connection.add(Steps.table, step)

    def get(self, name: str) -> Step:
        return self._connection.get(Steps.table, name)

    def get_all(self) -> list:
        return self._connection.get_all(Steps.table)

    def update(self, step: Step) -> None:
        self._connection.update(Steps.table, step)

    def delete(self, name: str) -> None:
        self._connection.delete(Steps.table, name)

    def delete_all(self) -> None:
        self._connection.delete_all(Steps.table)


# --------------------------------------------------------------------------- #
class Pipelines(Repository):
    """Repository for Pipeline Pipeline objects.

    Arguments:
        connection: PipelineDatabaseAccessObject.connection

    """

    table = 'pipelines'

    def __init__(self, connection):
        super(Pipelines, self).__init__(connection)

    def add(self, pipeline: Pipeline) -> None:
        self._connection.add(Pipelines.table, pipeline)

    def get(self, name: str) -> Pipeline:
        return self._connection.get(Pipelines.table, name)

    def get_all(self) -> list:
        return self._connection.get_all(Pipelines.table)

    def update(self, pipeline: Pipeline) -> None:
        self._connection.update(Pipelines.table, pipeline)

    def delete(self, name: str) -> None:
        self._connection.delete(Pipelines.table, name)

    def delete_all(self) -> None:
        self._connection.delete_all(Pipelines.table)
# --------------------------------------------------------------------------- #


class Events(Repository):
    """Repository for Pipeline Event objects.

    Arguments:
        connection: PipelineDatabaseAccessObject.connection

    """

    table = 'events'

    def __init__(self, connection):
        super(Events, self).__init__(connection)

    def add(self, event: Event) -> None:
        self._connection.add(Events.table, event)

    def get(self, name: str) -> Event:
        return self._connection.get(Events.table, name)

    def get_all(self) -> list:
        return self._connection.get_all(Events.table)

    def update(self, event: Event) -> None:
        self._connection.update(Events.table, event)

    def delete(self, name: str) -> None:
        self._connection.delete(Events.table, name)

    def delete_all(self) -> None:
        self._connection.delete_all(Events.table)


# --------------------------------------------------------------------------- #
class Repositories:
    """Encapsulates all repository classes.

    Dependency:
        database: PipelineDatabaseAccessObject

    """

    def __init__(self):
        self._database = PipelineDatabaseAccessObject()
        self._connection = self._database.get_connection()
        self._elements = Elements(self._connection)
        self._steps = Steps(self._connection)
        self._pipelines = Pipelines(self._connection)
        self._events = Events(self._connection)

    def open(self) -> None:
        self._elements.open()
        self._steps.open()
        self._pipelines.open()
        self._events.open()

    def close(self) -> None:
        self._elements.close()
        self._steps.close()
        self._pipelines.close()
        self._events.close()

    def save(self, event: Event) -> None:
        self._open()

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
