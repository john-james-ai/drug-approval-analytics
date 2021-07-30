# -*- coding:utf-8 -*-
# =========================================================================== #
# Project  : Drug Approval Analytics                                          #
# Version  : 0.1.0                                                            #
# File     : \src\platform\sqllib.py                                          #
# Language : Python 3.9.5                                                     #
# --------------------------------------------------------------------------  #
# Author   : John James                                                       #
# Company  : nov8.ai                                                          #
# Email    : john.james@nov8.ai                                               #
# URL      : https://github.com/john-james-sf/drug-approval-analytics         #
# --------------------------------------------------------------------------  #
# Created  : Saturday, July 24th 2021, 2:15:04 pm                             #
# Modified : Friday, July 30th 2021, 12:45:46 am                              #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause 'New' or 'Revised' License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
'''Module containing database administration query language.'''
from datetime import datetime

from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy import DateTime
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship

from .database import Base


# --------------------------------------------------------------------------- #
#                            EXECUTABLES                                      #
# --------------------------------------------------------------------------- #
class Executable(Base):
    __tablename__ = 'execution'
    id = Column(Integer, primary_key=True, autoincrement=True, nullable=True)
    type = Column(String(50))
    name = Column(String(32), index=True, nullable=False)
    description = Column(String(256))
    return_code = Column(Integer)
    return_value = Column(String(256))
    created = Column(DateTime, default=datetime.now)
    updated = Column(DateTime, onupdate=datetime.now)
    executed = Column(DateTime)

    __mapper_args__ = {'polymorphic_identity': 'artifact',
                       'polymorphic_on': type}

    def __init__(self, name):
        self.name = name


class Event(Executable):
    __tablename__ = 'event'
    id = Column(Integer, ForeignKey('execution.id'), primary_key=True)
    pipelines = relationship('Pipeline',
                             primaryjoin='(Event.id==Pipeline.event_id)')

    __mapper_args__ = {'polymorphic_identity': 'event'}

    def __init__(self, name):
        self.name = name

    @hybrid_property
    def return_code(self):
        return self.pipelines.return_code

    @hybrid_property
    def return_value(self):
        return self.pipelines.return_value


class Pipeline(Executable):
    __tablename__ = 'pipeline'
    id = Column(Integer, ForeignKey('execution.id'), primary_key=True)
    steps = relationship('Step')
    event_id = Column(Integer, ForeignKey('event.id'))
    steps = relationship('Step',
                         primaryjoin='(Pipeline.id == Step.pipeline_id)')

    __mapper_args__ = {'polymorphic_identity': 'pipeline'}

    def __init__(self, name):
        self.name = name

    @hybrid_property
    def return_code(self):
        return max(step.return_code for step in self.steps)

    @hybrid_property
    def return_value(self):
        if self.executed:
            return_code = 0
            self.return_value = None
            for step in self.steps:
                if step.return_code > return_code:
                    return_code = step.return_code
                    self.return_value = step.return_value
            return self.return_value


class Step(Executable):
    __tablename__ = 'step'
    id = Column(Integer, ForeignKey('execution.id'), primary_key=True)
    pipeline_id = Column(Integer, ForeignKey('pipeline.id'))
    step_input_id = Column(Integer, ForeignKey('artifact.id'))
    step_output_id = Column(Integer, ForeignKey('artifact.id'))
    step_input = relationship('Artifact', foreign_keys=[step_input_id])
    step_output = relationship('Artifact', foreign_keys=[step_output_id])

    __mapper_args__ = {'polymorphic_identity': 'step'}

    def __init__(self, name):
        self.name = name

    @hybrid_property
    def step_input(self):
        return self.step_input.name

    @hybrid_property
    def step_output(self):
        return self.step_output.name

# --------------------------------------------------------------------------- #
#                               ARTIFACT                                      #
# --------------------------------------------------------------------------- #


class Artifact(Base):
    __tablename__ = 'artifact'
    id = Column(Integer, primary_key=True, autoincrement=True, nullable=True)
    type = Column(String(50))
    name = Column(String(32), index=True, nullable=False)
    version = Column(Integer)
    description = Column(String(256))
    uri = Column(String(256))
    uri_kind = Column(String(32))
    created = Column(DateTime, default=datetime.now)
    updated = Column(DateTime, onupdate=datetime.now)
    datasources = relationship("DataSource",
                               primaryjoin='(Artifact.id==\
                                    DataSource.id)')

    __mapper_args__ = {'polymorphic_identity': 'artifact',
                       'polymorphic_on': type}

    def __init__(self, name):
        self.name = name


class DataSource(Artifact):
    __tablename__ = 'datasource'
    id = Column(Integer, ForeignKey('artifact.id'), primary_key=True)
    creator = Column(String(256))
    webpage = Column(String(256))
    media_type = Column(String(64))
    frequency = Column(Integer)
    lifecycle = Column(Integer)
    extracted = Column(DateTime)
    entities = relationship('Entity',
                            primaryjoin='(DataSource.id==\
                                Entity.datasource_id)')

    __mapper_args__ = {'polymorphic_identity': 'datasource'}


class Entity(Artifact):
    __tablename__ = 'entity'
    id = Column(Integer, ForeignKey('artifact.id'), primary_key=True)
    num_entity_samples = Column(Integer)
    datasource_id = Column(Integer, ForeignKey('datasource.id'))
    features = relationship('Feature',
                            primaryjoin='(Entity.id==Feature.entity_id)')

    __mapper_args__ = {'polymorphic_identity': 'entity'}


class Feature(Artifact):
    __tablename__ = 'feature'
    id = Column(Integer, ForeignKey('artifact.id'), primary_key=True)
    datatype = Column(String(32))
    num_feature_samples = Column(Integer)
    num_null_feature_values = Column(Integer)
    entity_id = Column(Integer, ForeignKey('entity.id'))

    __mapper_args__ = {'polymorphic_identity': 'feature'}

    @hybrid_property
    def pct_null_values(self):
        if self.num_null_values:
            return self.num_null_values / self.num_samples * 100
        else:
            return 0
