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
# Modified : Saturday, July 31st 2021, 10:00:07 am                            #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
# %%
"""Metadata Database (Metabase) Module defining the project metadata model.

The metabase contains information about everything that happens in the
project from data acquisition, through feature engineering and
model development. As such, the metabase is a collection of artifacts,
defined loosely as physical products and byproducts of the data
science process.

There are four types of artifacts defined, and persisted in this
project: data artifacts, statistics artifacts, event artifacts, 
and model artifacts.  Data artifacts consists of data sources, 
data sets, and databases, physical data objects that are processed 
and analyzed. Statistics artifacts are descriptive and are 
generated during data processing and cleaning operations.
Event artifacts capture the methods or tasks performed in terms of
time, duration, computing resources consumed, and results 
of those operatoins. Finally, model artifacts
include the fully trained models, predictions, model hyper
parameters and performance metrics. The classes are:

    Data Artifacts:
        - DataSourceArt
        - DataSetArt
        - DataBaseArt
        - EntityArt
        - FeatureArt

    Stats Artifacts:
        - StatsFrequencyArt
        - StatsPositionArt
        - StatsCentralityArt
        - StatsDispersionArt

    Event Artifacts:
        - DataExtractArt
        - DataProcessArt
        - DataLoadingArt
        - ModelTrainArt
        - ModelValidationArt

    Model Artifacts:
        - ModelParamsArt
        - ModelPerformanceArt
        - ModelTrainedArt

These artifacts are defined as dataclasses with an "Art" suffix
designating the class as an artifact class. Concrete artifact
objects are created and injected as a dependency to other
more fully specified classes  encapsulating behavioral,
structural  and creational facets.

"""
from abc import ABC, abstractmethod
from datetime import datetime
from uuid import uuid4
from types import Union, List
import pandas as pd


# =========================================================================== #
#                               ARTIFACTS                                     #
# =========================================================================== #
# --------------------------------------------------------------------------- #
#                            DATA ARTIFACTS                                   #
# --------------------------------------------------------------------------- #
@dataclass
class Artifact(ABC):

    id: uuid.UUID
    name: str
    type: str
    version: int
    description: str
    created: datetime
    updated: datetime

    def on_save(self):
        self.version += 1
        self.updated = datetime.now()


class DataArt(Artifact):
    """Base class for data artifacts."""
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    uri_type: str = "website"
    created: datetime = field(default_factory=datetime.now)
    updated: datetime = datetime.fromtimestamp(0)


# --------------------------------------------------------------------------- #
@dataclass
class DataSetArt(DataArt):
    """Extends the base class to define data set artifacts."""
    format: str = None
    size: int = 0


# --------------------------------------------------------------------------- #
@dataclass
class DatabaseArt(DataArt):
    """Defines the database Artifact."""
    database: str = 'postgres'                  # The database used
    tables: list = field(default_factory=list)  # List of tables
    size: int = 0                               # Size of the database


# --------------------------------------------------------------------------- #
class DataSourceArt(DataArt):
    """Defines a DataSource and extraction behaviors."""

    webpage: str = None
    creator: str = None
    media_type: str = None
    frequency: str = None
    lifecycle: int = 7
    posted: datetime = datetime.fromtimestamp(0)
    extracted: datetime = datetime.fromtimestamp(0)


# --------------------------------------------------------------------------- #
@dataclass
class EntityArt(DataArt):
    """Entity Artifacts."""
    num_entities: int = 0
    stage: str = 'raw'


# --------------------------------------------------------------------------- #
@dataclass
class FeatureArt(DataArt):
    """Entity Artifacts."""
    entity_id: uuid = field(init=False)
    datatype: str = None
    datakind: str = None
    stage: str = 'raw'


# --------------------------------------------------------------------------- #
#                           STATS ARTIFACTS                                   #
# --------------------------------------------------------------------------- #
class StatsArt(ABC):

    @abstractmethod
    def compute(self, df: pd.DataFrame) -> pd.DataFrame:
        pass


# --------------------------------------------------------------------------- #
class StatsFreqArt(StatsArt):
    """Frequency, count, percentage statistics."""

    def compute(self, df: pd.DataFrame) -> pd.DataFrame:
        pass

# --------------------------------------------------------------------------- #


class StatsPositionArt(StatsArt):
    """Percentile and quartile ranks."""

    def compute(self, df: pd.DataFrame) -> pd.DataFrame:
        pass

# --------------------------------------------------------------------------- #


class StatsCentralityArt(StatsArt):
    """Percentile and quartile ranks."""

    def compute(self, df: pd.DataFrame) -> pd.DataFrame:
        pass


# --------------------------------------------------------------------------- #
class StatsDispersionArt(StatsArt):
    """Range, variance, standard deviation."""

    def compute(self, df: pd.DataFrame) -> pd.DataFrame:
        pass


# --------------------------------------------------------------------------- #
#                           EVENT ARTIFACTS                                   #
# --------------------------------------------------------------------------- #
class EventArtifact(Artifact):
    """Artifacts for data and modeling events."""

    event_id: uuid = None
    event_input_id: uuid = None
    event_output_id: uuid = None
    event_params_id: uuid = None
    event_start: datetime = datetime.fromtimestamp(0)
    event_end:  datetime = datetime.fromtimestamp(0)
    event_duration: int = 0
    event_return_code: int = 0
    event_return_value: None


# --------------------------------------------------------------------------- #
#                           MODEL ARTIFACTS                                   #
# --------------------------------------------------------------------------- #
# TODO: Create parameter and model artifacts that include dataset statistics,
# hyperparameters, algorithm, and performance metrics.


def main():
    ds = DataSource(name="joe", type="www.com", lifecycle=4)
    print(ds)


if __name__ == '__main__':
    main()
