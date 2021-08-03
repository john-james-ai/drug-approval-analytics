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
# Modified : Tuesday, August 3rd 2021, 4:34:43 am                             #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
# %%
"""Metadata Database (Metabase) Module defining the project metadata model."""
from sqlalchemy import String, Integer, Column, Identity, DateTime
from sqlalchemy import ForeignKey, Float, relationship, ARRAY, Boolean
from .database import Base


# --------------------------------------------------------------------------- #
#                              ARTIFACTS                                      #
# --------------------------------------------------------------------------- #
class Artifact(Base):
    """Base class for all metabase artifacts."""
    __tablename__ = "artifact"
    id = Column(Integer, Identity, primary_key=True)
    type = Column(String(50))
    version = Column(Integer, nullable=False)
    name = Column(String(32))
    description = Column(String(256))
    creator = Column(String(64))
    created = Column(DateTime)
    updated = Column(DateTime)

    __mapper__ = {
        'polymorphic_identity': 'artifact',
        'polymorphic_on': type,
        "version_id_col": version,
    }


# --------------------------------------------------------------------------- #
class DataSource(Artifact):
    """Defines external data sources."""
    uri = Column(String(256))
    uri_type = Column(String(32))
    lifecycle = Column(Integer)
    extracted = Column(DateTime)
    __mapper_args__ = {
        'polymorphic_identity': 'datasource'
    }


# --------------------------------------------------------------------------- #
class DataSet(Artifact):
    """Defines a physical data set."""
    uri = Column(String(256))
    uri_type = Column(String(32))
    derived_from = Column(Integer)
    statistics = relationship("Statistic")
    __mapper_args__ = {
        'polymorphic_identity': 'dataset'
    }


# --------------------------------------------------------------------------- #
class Parameter(Artifact):
    """Defines a physical data set."""
    value_int = Column(Integer)
    value_float = Column(Float)
    value_bool = Column(Boolean)
    value_str = Column(String(32))

    __mapper_args__ = {
        'polymorphic_identity': 'parameter'
    }


# --------------------------------------------------------------------------- #
#                               PIPELINE                                      #
# --------------------------------------------------------------------------- #
class Sequence(Base):
    """A sequence of events."""
    __tablename__ = 'sequence'
    id = Column(Integer, Identity, primary_key=True, nullable=False)
    name = Column(String(50))
    events = relationship("Event", back_populates="sequence")
    sequence_started = Column(DateTime)
    sequence_ended = Column(DateTime)
    sequence_duration = Column(Integer)
    sequence_return_code = Column(Integer)
    sequence_return_value = Column(String(128))


# --------------------------------------------------------------------------- #
class EventInput(Base):
    __tablename__ = "eventinput"
    left_id = Column(ForeignKey('event.id'), primary_key=True)
    right_id = Column(ForeignKey('artifact.id'), primary_key=True)
    assigned = Column(DateTime)
    artifact = relationship("Artifact")


# --------------------------------------------------------------------------- #
class EventOutput(Base):
    __tablename__ = "eventoutput"
    left_id = Column(ForeignKey('event.id'), primary_key=True)
    right_id = Column(ForeignKey('artifact.id'), primary_key=True)
    assigned = Column(DateTime)
    artifact = relationship("Artifact")


# --------------------------------------------------------------------------- #
class EventParam(Base):
    __tablename__ = "eventparams"
    left_id = Column(ForeignKey('event.id'), primary_key=True)
    right_id = Column(ForeignKey('artifact.id'), primary_key=True)
    assigned = Column(DateTime)
    artifact = relationship("Artifact")

# --------------------------------------------------------------------------- #


class Event(Base):
    """Defines a event in a pipeline."""
    __tablename__ = "event"
    id = Column(Integer, Identity, primary_key=True)
    name = Column(String(32))
    description = Column(String(256))
    event_input = relationship("EventInput")
    event_output = relationship("EventOutput")
    event_params = relationship("EventParam")
    started = Column(DateTime)
    ended = Column(DateTime)
    duration = Column(Integer)
    return_code = Column(Integer)
    return_value = Column(String(128))
    sequence_id = Column(Integer, ForeignKey("sequence.id"))
    sequence = relationship("Sequence", back_populates="events")


# --------------------------------------------------------------------------- #
#                              STATISTICS                                     #
# --------------------------------------------------------------------------- #
class Statistic(Base):
    """Descriptive descriptive for a dataset."""
    __tablename__ = "statistic"
    id = Column(Integer, Identity, primary_key=True)
    name = Column(String(32))
    description = Column(String(256))
    value = Column(Float)
    # The columns to which the statistic applies. Supports univariate
    # and bivariate statistics at the dataset and column level. The array
    # is empty for dataset level stats.
    columns = Column(ARRAY(String(64)))
    creator = Column(String(64))
    created = Column(DateTime)
    dataset_id = Column(Integer, ForeignKey('dataset.id'))
