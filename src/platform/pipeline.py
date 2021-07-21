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
# Modified : Wednesday, July 21st 2021, 1:31:28 pm                            #
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
    events: Events repository unit of work class.

"""
from abc import ABC, abstractmethod
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
        self._name = name
        self._uri = uri
        self._description = description

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

    """

    def __init__(self, name: str, description: str,
                 step_in: Element, step_out: Element) -> None:
        self._name = name
        self._description = description
        self._step_in = step_in
        self._step_out = step_out
        self._return_code = 0
        self._return_description = None

    @abstractmethod
    def execute(self, *args, **kwargs) -> None:
        pass

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
    def return_code(self) -> int:
        return self._return_code

    @property
    def return_description(self) -> str:
        return self._return_description
# --------------------------------------------------------------------------- #


class Pipeline:
    """Engine that performs tasks by executing Step objects.

    Arguments:
        name (str): A short human readabile string describing the pipeline
        description (str): Sting describing the pipeline
        stage (str): String characterizing the type of pipeline.

    Attributes:
        name (str): A short human readabile string describing the pipeline
        description (str): Sting describing the pipeline
        stage (str): String characterizing the type of pipeline.

    """

    def __init__(self, name: str, stage: str, description: str) -> None:
        self._name = name
        self._description = description
        self._stage = stage
        self._steps = []

    def add_step(self, step: Step) -> None:
        self._steps.append(step)

    def execute(self):
        for step in self._steps:
            step.execute()

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
