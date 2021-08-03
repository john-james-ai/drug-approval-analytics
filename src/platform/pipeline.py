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
# Modified : Saturday, July 24th 2021, 3:56:38 am                             #
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
from uuid import uuid4, UUID
from abc import ABC, abstractmethod
from datetime import datetime

from src.platform.repository import Repositories
from ..utils.logger import exception_handler, logger
# -----------------------------------------------------------------------------#


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

    def __str__(self):
        text = "\n"
        text += "                            Input Element Id: " + \
            str(self._id) + "\n"
        text += "                                        Name: " + \
            str(self._name) + "\n"
        text += "                                 Description: " + \
            str(self._description) + "\n"

        text += "# -------------------------------------"
        text += "-------------------------------------- #\n"
        return text

    def __repr__(self):
        return f'Element(name={self._name}, uri={self._uri}, \
            description={self._description})'

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
        self._step_in.step_id = self._id
        self._step_out.step_id = self._id
        self._started = None
        self._stopped = None
        self._return = Return()
        self._pipeline_id = None

    def __str__(self):
        text = "\n"
        text += "                         Step Id: " + str(self._id) + "\n"
        text += "                               Name: " + \
            str(self._name) + "\n"
        text += "                        Description: " + \
            str(self._description) + "\n"
        text += "                            Started: " + \
            str(self._started) + "\n"
        text += "                              Ended: " + \
            str(self._stopped) + "\n"
        text += "                        Return Code: " + \
            str(self._return.code) + "\n"
        text += "                              State: " + \
            str(self._return.description) + "\n"

        text += "# -------------------------------------"
        text += "-------------------------------------- #\n"
        text += self._step_in.__str__
        text += self._step_out.__str__
        return text

    def __repr__(self):
        return f'Step(name={self._name}, description={self._description}, \
            step_in={self._step_in}, step_out={self._step_out})'

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

    @exception_handler()
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

    @property
    def pipeline_id(self) -> UUID:
        return self._pipeline_id

    @pipeline_id.setter
    def pipeline_id(self, pipeline_id: UUID) -> None:
        self._pipeline_id = pipeline_id


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
        self._steps = {}
        self._return = Return()
        self._started = None
        self._stopped = None

    def __str__(self):
        text = "\n"
        text += "             Pipeline Id: " + str(self._id) + "\n"
        text += "                    Name: " + str(self._name) + "\n"
        text += "                   Stage: " + str(self._stage) + "\n"
        text += "             Description: " + str(self._description) + "\n"
        text += "# -------------------------------------"
        text += "-------------------------------------- #\n"
        for step in self._steps:
            text += step.__str__()
        return text

    def __repr__(self):
        return f'Pipeline(name={self._name}, description={self._description}, \
            stage={self._stage})'

    def add_step(self, step: Step) -> None:
        step.pipeline_id = self._id
        self._steps[step.name] = step

    def get_step(self, name: str) -> None:
        try:
            return self._steps.get(name)
        except Exception as e:
            logger.error(e)

    def get_steps(self) -> dict:
        return self._steps

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
        return self._return

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
    def steps(self) -> dict:
        return self._steps

    @property
    def started(self) -> str:
        return self._started

    @property
    def stopped(self) -> str:
        return self._stopped

    @property
    def event_id(self) -> UUID:
        return self._event_id

    @event_id.setter
    def event_id(self, event_id: UUID) -> None:
        self._event_id = event_id

    @property
    def return_value(self) -> Return:
        return self._return

# --------------------------------------------------------------------------- #


class Event:
    """An execution of a Pipeline object."""

    def __init__(self, pipeline: Pipeline) -> None:
        self._id = uuid4()
        self._pipeline = pipeline
        self._pipeline.event_id = self._id
        self._started = None
        self._stopped = None
        self._return = Return()

    def __str__(self):
        text = "\n"
        text += "        Event Id: " + str(self._id) + "\n"
        text += "# -------------------------------------"
        text += "-------------------------------------- #\n"
        text += "         Started: " + str(self._started)
        text += "           Ended: " + str(self._started)
        text += "     Return Code: " + self._return.code
        text += "           State: " + self._return.description
        text += "# -------------------------------------"
        text += "-------------------------------------- #\n"
        text += self._pipeline.__str__()
        return text

    def __repr__(self):
        return f'Event(pipeline={self._pipeline})'

    def execute(self) -> None:
        self._started = datetime.now()
        self._return = self._pipeline.execute()
        self._stopped = datetime.now()
        return self._return

    @property
    def return_value(self) -> Return:
        return self._return

    @property
    def pipeline(self):
        return self._pipeline


class PipelineManager:
    """Encapsulates a Pipeline execution.

    Wraps the Pipeline class inside an Event. Decoupling the Pipeline from
    the Event allows one to execute a particular Pipeline multiple times.

    Arguments:
        pipeline(Pipeline): Non executed Pipeline object.

    """

    def __init__(self, repositories: Repositories) -> None:
        self._repositories = repositories

    def __str__(self):
        return f'Event(repositories={self._repositories})'

    def __repr__(self):
        return f'Event(repositories={self._repositories})'

    def create_event(self, pipeline: Pipeline) -> Event:
        self._event = Event(pipeline)

    def execute(self) -> None:
        self._event.pipeline.execute()

    def save(self) -> None:
        self._repositories.save(self._event)

    def get_event(self, name: str) -> Event:
        return self._repositories.events.get(name)

    def get_events(self) -> Event:
        return self._repositories.events.get_all()

    def delete_event(self, name: str) -> None:
        self._repositories.event.delete(name)

    def delete_events(self) -> None:
        self._repositories.event.delete_all()
