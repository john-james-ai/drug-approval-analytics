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
# Modified : Wednesday, July 21st 2021, 2:09:30 pm                            #
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
from .pipeline import Element  # Add Step, Pipeline
# from .database import PipelineDatabaseAccessObject

# --------------------------------------------------------------------------- #


class Elements:
    """Repository for Pipeline Element objects.

    Arguments:
        database: PipelineDatabaseAccessObject

    """
    def __init__(self, database):
        self._database = database

    def add(self, element: Element) -> None:
        self._database.add_element(element)

    def get(self, name: str) -> Element:
        return self._database.get_element(name)

    def get_all(self) -> list:
        return self._database.get_all_elements()

    def update(self, element) -> None:
        self._database.update_element(element)

    def delete(self, name: str) -> None:
        self._database.delete_element(name)