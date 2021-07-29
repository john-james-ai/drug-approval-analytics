#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# =========================================================================== #
# Project  : Drug Approval Analytics                                          #
# Version  : 0.1.0                                                            #
# File     : \src\platform\metadata.py                                        #
# Language : Python 3.9.5                                                     #
# --------------------------------------------------------------------------  #
# Author   : John James                                                       #
# Company  : nov8.ai                                                          #
# Email    : john.james@nov8.ai                                               #
# URL      : https://github.com/john-james-sf/drug-approval-analytics         #
# --------------------------------------------------------------------------  #
# Created  : Saturday, July 24th 2021, 10:16:36 pm                            #
# Modified : Saturday, July 24th 2021, 11:45:01 pm                            #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
"""Module for defining, recording and accessing metadata for workflows."""
from dataclasses import dataclass, field
# --------------------------------------------------------------------------- #


@dataclass
class PropertySchema:
    name: str
    datatype: str
    not_null: bool = field(default=False)
    unique: bool = field(default=False)
    primary_key: bool = field(default=False)

    def get_column_snip(self) -> tuple:
        constraint = self.datatype
        constraint += " NOT NULL" if self.not_null else ""
        constraint += " UNIQUE" if self.unique else ""
        constraint += " PRIMARY KEY" if self.primary_key else ""
        self._column = (self.name, constraint)

    @property
    def column(self) -> tuple:
        return self._column


class ArtifactType:
    """Defines the a collection of artifacts and their properties."""

    def __init__(self, name: str, description: str = None) -> None:
        self.name = name
        self.description = description
        self._properties = []

    def add_property_schema(self, property_schema):
        self._properties.append(property_schema)
