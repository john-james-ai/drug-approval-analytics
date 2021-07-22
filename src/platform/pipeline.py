#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# =========================================================================== #
# Project  : Drug Approval Analytics                                          #
# Version  : 0.1.0                                                            #
# File     : \src\pipeline\core.py                                            #
# Language : Python 3.9.5                                                     #
# --------------------------------------------------------------------------  #
# Author   : John James                                                       #
# Company  : nov8.ai                                                          #
# Email    : john.james@nov8.ai                                               #
# URL      : https://github.com/john-james-sf/drug-approval-analytics         #
# --------------------------------------------------------------------------  #
# Created  : Wednesday, July 21st 2021, 9:27:37 am                            #
# Modified : Wednesday, July 21st 2021, 10:22:21 pm                           #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
"""Pipeline

This module defines the core components of the pipeline framework, which
include:

    - Element: Base clas for input and output elements of each step.
    - Step: Pipeline task
    - Pipeline: The engine that executes the steps.

Example:
    Insert here...

Attributes:
    No module level attributes

Dependencies:
    repositories (Repository): Container of repository classes

"""
from dataclasses import dataclass, field
import logging
import UUID
from uuid import uuid4
from abc import ABC, abstractmethod
from datetime import datetime

from .repository import Repositories
from .monitor import exception_handler
# -----------------------------------------------------------------------------#
logging.setLevel(logging.NOTSET)
logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #


@dataclass
class Return:
    code: int = field(default=100)
    description: str = field(default='Continue')


# -----------------------------------------------------------------------------#
class Element(ABC):
    """Abstract base class for all pipeline step input/output objects.add()

    A pipeline element contains the metadata for the actual pipeline
    object.

    Arguments:
        name (str): A short human readabile string describing the element.
        description (str): Sting describing the element.
        uri (str): A uniform resource identifier for the object.

    Attributes:
        name (str): A short human readabile string describing the element.
        description (str): Sting describing the element.
        uri (str): A uniform resource identifier for the object.

    """

    def __init__(self, name: str, uri: str, description: str) -> None:
        self._id = uuid4()
        self._name = name
        self._uri = uri
        self._description = description

    @property
    def id(self) -> UUID:
        return self._id

    @property
    def name(self) -> str:
        return self._name

    @property
    def uri(self) -> str:
        return self._uri

    @uri.setter
    def uri(self, uri: str) -> None:
        self._uri = uri

    @property
    def description(self) -> str:
        return self._description

    @description.setter
    def description(self, description: str) -> None:
        self._description = description


# --------------------------------------------------------------------------- #


class Step(ABC):
    """Abstract base class for pipeline tasks

    Arguments:
        name (str): A short human readabile string describing the step.
        description (str): Sting describing the step
        step_in (Element): The input element for the step
        step_out (Element): The output element for the step

    Attributes:
        name (str): A short human readabile string describing the step.
        description (str): Sting describing the step
        step_in (Element): The input element for the step
        step_out (Element): The output element for the step
        return_code (int): Zero if successful, non zero of not.
        return_description (str): Additional return state information
        started (datetime): Timestamp marking the beginning of an execution.
        stopped (datetime): Timestamp marking the ending of an execution.

    """

    def __init__(self, name: str, description: str,
                 step_in: Element, step_out: Element) -> None:
        self._id = uuid4()
        self._name = name
        self._description = description
        self._step_in = step_in
        self._step_out = step_out
        self._started = None
        self._stopped = None
        self._return = Return()

    def _start(self) -> None:
        self._started = datetime.now()

    def _stop(self) -> None:
        self._stopped = datetime.now()

    @abstractmethod
    def _run(self) -> None:
        # Subclasses must decorate this method with the 'exception_handler'
        # decorator. They must also include object specific treatment
        # of anomalies.
        pass

    @exception_handler
    def execute(self) -> Return:
        self._start()
        self._run()
        self._stop()
        return self._return

    @property
    def id(self) -> UUID:
        return self._id

    @property
    def name(self) -> Element:
        return self._name

    @property
    def description(self) -> Element:
        return self._description

    @description.setter
    def description(self, description: str) -> None:
        self._description = description

    @property
    def step_in(self) -> Element:
        return self._step_in

    @step_in.setter
    def step_in(self, step_in: Element) -> None:
        self._step_in = step_in

    @property
    def step_out(self) -> Element:
        return self._step_out

    @step_out.setter
    def step_out(self, step_out: Element) -> None:
        self._step_out = step_out

    @property
    def return_value(self) -> Return:
        return self._return

    @property
    def started(self) -> str:
        return self._started

    @property
    def stopped(self) -> str:
        return self._stopped


# --------------------------------------------------------------------------- #
class Pipeline:
    """Pipeline class that encapsulates a series of tasks.

    Arguments:
        name (str): A short human readabile string describing the pipeline
        description (str): Sting describing the pipeline
        stage (str): String characterizing the type of pipeline.

    Attributes:
        name (str): A short human readabile string describing the pipeline
        description (str): Sting describing the pipeline
        stage (str): String characterizing the type of pipeline.
        started (datetime): Timestamp marking the beginning of an execution.
        stopped (datetime): Timestamp marking the ending of an execution.

    """

    def __init__(self, name: str, stage: str, description: str) -> None:
        self._id = uuid4()
        self._name = name
        self._description = description
        self._stage = stage
        self._steps = []
        self._return = Return()
        self._started = None
        self._stopped = None

    def add_step(self, step: Step) -> None:
        self._steps.append(step)

    def _start(self) -> None:
        self._started = datetime.now()

    def _stop(self) -> None:
        self._stopped = datetime.now()

    def _run(self) -> None:
        for step in self._steps:
            self._return = step.execute()

    def execute(self):
        self._start()
        self._run()
        self._stop()

    @property
    def id(self) -> UUID:
        return self._id

    @property
    def name(self) -> str:
        return self._name

    @property
    def description(self) -> str:
        return self._description

    @description.setter
    def description(self, description: str) -> None:
        self._description = description

    @property
    def stage(self) -> str:
        return self._stage

    @stage.setter
    def stage(self, stage: str) -> None:
        self._stage = stage

    @property
    def started(self) -> str:
        return self._started

    @property
    def stopped(self) -> str:
        return self._stopped

    @property
    def return_value(self) -> Return:
        return self._return_value

# --------------------------------------------------------------------------- #


class Event():
    """Encapsulates a Pipeline execution.

    Wraps the Pipeline class inside an Event. Decoupling the Pipeline from
    the Event allows one to execute a particular Pipeline multiple times.

    Arguments:
        pipeline(Pipeline): Non executed Pipeline object.

    """

    def __init__(self, pipeline: Pipeline) -> None:
        self._id = uuid4()
        self._pipeline = pipeline
        self._return = Return()
        self._repositories = Repositories()

    def execute(self) -> None:
        self._return = self._pipeline.execute()

    def save(self) -> None:
        # TODO: repository update
        pass

    @property
    def id(self) -> UUID:
        return self._id

    @property
    def pipeline(self):
        return self._pipeline

    @property
    def return_value(self) -> Return:
        return self._return
