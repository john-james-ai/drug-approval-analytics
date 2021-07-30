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
# Modified : Friday, July 30th 2021, 1:02:52 am                               #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
"""Repository for artifacts and executables

This module defines the repository for artifacts and executables:

    - Repository: Base class for the following subclasses.
    - ArtifactRepo: DataSource, Dataset, Entity, and Feature artifacts.
    - StepRepo: Repository database administration
    - PipelineRepo: Repository database access object.
    - EventRepo: Repository class interface used by the Pipeline


"""
from abc import ABC, abstractmethod
from typing import Union, List

from .core import Artifact, DataSource, Entity, Feature
from .core import Executable, Event, Pipeline, Step


# --------------------------------------------------------------------------- #
#                              REPOSITORY                                     #
# --------------------------------------------------------------------------- #


class Repository(ABC):
    """Base class for Repository subclasses.

    Arguments:
        database: ORMDatabase object

    """

    table = 'events'

    def __init__(self, database):
        self._database = database.database
        self._session = database.session()

    @abstractmethod
    def add(self, **Kwargs) -> int:
        pass

    @abstractmethod
    def add_all(self, **Kwargs) -> int:
        pass

    @abstractmethod
    def get_by_id(self, **Kwargs) -> Union[
            Artifact, DataSource, Entity, Feature,
            Executable, Event, Pipeline, Step]:
        pass

    @abstractmethod
    def get_by_name(self, **kwargs) -> Union[
            Artifact, DataSource, Entity, Feature,
            Executable, Event, Pipeline, Step]:
        pass

    @abstractmethod
    def get_all(self) -> list:
        pass

    @abstractmethod
    def delete(self, name: str) -> None:
        pass

    @abstractmethod
    def delete_all(self) -> None:
        pass

    def print_all(self) -> None:
        print(self.get_all())

# --------------------------------------------------------------------------- #
#                         ARTIFACT REPOSITORY                                 #
# --------------------------------------------------------------------------- #


class ArtifactRepo(Repository):
    """Database administration for the repository database."""

    def add(self, artifact: Artifact) -> int:
        self._session.add(artifact)
        self._session.flush()
        self._commit()
        return artifact.id

    def add_all(self, artifacts: List[Artifact]) -> List[int]:
        ids = []
        for artifact in artifacts:
            ids.append(self.add(artifact))
        return ids

    def get_by_id(self, artifact_id: int) -> Artifact:
        artifact = self._session.query(Artifact).filter(
            Artifact.id == artifact_id).all()
        return artifact

    def get_by_name(self, artifact_name: str) -> Artifact:
        artifact = self._session.query(Artifact).filter(
            Artifact.name == artifact_name).all()
        return artifact

    def get_all(self) -> list:
        return self._session.query(Artifact).all()

    def delete(self, name: str) -> None:
        self._session.query(Artifact).filter(Artifact.name == name).delete()
        self._session.commit()

    def delete_all(self) -> None:
        self._session.query(Artifact).delete()
        self._session.commit()

# --------------------------------------------------------------------------- #
#                        DATASOURCE REPOSITORY                                #
# --------------------------------------------------------------------------- #


class DataSourceRepo(Repository):
    """Database administration for the repository database."""

    def add(self, datasource: DataSource) -> int:
        self._session.add(datasource)
        self._session.flush()
        self._session.commit()
        return datasource.id

    def add_all(self, datasources: List[DataSource]) -> List[int]:
        ids = []
        for datasource in datasources:
            ids.append(self.add(datasource))
        self._session.commit()
        return ids

    def get_by_id(self, datasource_id: int) -> DataSource:
        datasource = self._session.query(DataSource).filter(
            DataSource.id == datasource_id).all()
        return datasource

    def get_by_name(self, datasource_name: str) -> DataSource:
        datasource = self._session.query(DataSource).filter(
            DataSource.name == datasource_name).all()
        return datasource

    def get_all(self) -> list:
        return self._session.query(DataSource).all()

    def delete(self, name: str) -> None:
        self._session.query(DataSource).filter(
            DataSource.name == name).delete()
        self._session.commit()

    def delete_all(self) -> None:
        self._session.query(DataSource).delete()
        self._session.commit()


# --------------------------------------------------------------------------- #
#                           ENTITY REPOSITORY                                 #
# --------------------------------------------------------------------------- #
class EntityRepo(Repository):
    """Database administration for the repository database."""

    def add(self, entity: Entity) -> int:
        self._session.add(entity)
        self._session.flush()
        return entity.id

    def add_all(self, entities: List[Entity]) -> List[int]:
        ids = []
        for entity in entities:
            ids.append(self.add(entity))
        self._session.commit()
        return ids

    def get_by_id(self, entity_id: int) -> Entity:
        entity = self._session.query(Entity).filter(
            Entity.id == entity_id).all()
        return entity

    def get_by_name(self, entity_name: str) -> Entity:
        entity = self._session.query(Entity).filter(
            Entity.name == entity_name).all()
        return entity

    def get_all(self) -> list:
        return self._session.query(Entity).all()

    def delete(self, name: str) -> None:
        self._session.query(Entity).filter(Entity.name == name).delete()
        self._session.commit()

    def delete_all(self) -> None:
        self._session.query(Entity).delete()
        self._session.commit()


# --------------------------------------------------------------------------- #
#                           FEATURE REPOSITORY                                #
# --------------------------------------------------------------------------- #
class FeatureRepo(Repository):
    """Database administration for the repository database."""

    def add(self, feature: Feature) -> int:
        self._session.add(feature)
        self._session.flush()
        return feature.id

    def add_all(self, features: List[Feature]) -> List[int]:
        ids = []
        for feature in features:
            ids.append(self.add(feature))
        self._session.commit()
        return ids

    def get_by_id(self, feature_id: int) -> Feature:
        feature = self._session.query(Feature).filter(
            Feature.id == feature_id).all()
        return feature

    def get_by_name(self, feature_name: str) -> Feature:
        feature = self._session.query(Feature).filter(
            Feature.name == feature_name).all()
        return feature

    def get_all(self) -> list:
        return self._session.query(Feature).all()

    def delete(self, name: str) -> None:
        self._session.query(Feature).filter(Feature.name == name).delete()
        self._session.commit()

    def delete_all(self) -> None:
        self._session.query(Feature).delete()
        self._session.commit()


# --------------------------------------------------------------------------- #
#                          EXECUTABLE REPOSITORY                              #
# --------------------------------------------------------------------------- #
class ExecutableRepo(Repository):
    """Database administration for the repository database."""

    def add(self, executable: Executable) -> int:
        self._session.add(executable)
        self._session.flush()
        return executable.id

    def add_all(self, executables: List[Executable]) -> List[int]:
        ids = []
        for executable in executables:
            ids.append(self.add(executable))
        self._session.commit()
        return ids

    def get_by_id(self, executable_id: int) -> Executable:
        executable = self._session.query(Executable).filter(
            Executable.id == executable_id).all()
        return executable

    def get_by_name(self, executable_name: str) -> Executable:
        executable = self._session.query(Executable).filter(
            Executable.name == executable_name).all()
        return executable

    def get_all(self) -> list:
        return self._session.query(Executable).all()

    def delete(self, name: str) -> None:
        self._session.query(Executable).filter(
            Executable.name == name).delete()
        self._session.commit()

    def delete_all(self) -> None:
        self._session.query(Executable).delete()
        self._session.commit()


# --------------------------------------------------------------------------- #
#                          EVENT REPOSITORY                                   #
# --------------------------------------------------------------------------- #
class EventRepo(Repository):
    """Database administration for the repository database."""

    def add(self, event: Event) -> int:
        self._session.add(event)
        self._session.flush()
        return event.id

    def add_all(self, events: List[Event]) -> List[int]:
        ids = []
        for event in events:
            ids.append(self.add(event))
        self._session.commit()
        return ids

    def get_by_id(self, event_id: int) -> Event:
        event = self._session.query(Event).filter(
            Event.id == event_id).all()
        return event

    def get_by_name(self, event_name: str) -> Event:
        event = self._session.query(Event).filter(
            Event.name == event_name).all()
        return event

    def get_all(self) -> list:
        return self._session.query(Event).all()

    def delete(self, name: str) -> None:
        self._session.query(Event).filter(
            Event.name == name).delete()
        self._session.commit()

    def delete_all(self) -> None:
        self._session.query(Event).delete()
        self._session.commit()


# --------------------------------------------------------------------------- #
#                           PIPELINE REPOSITORY                               #
# --------------------------------------------------------------------------- #
class PipelineRepo(Repository):
    """Database administration for the repository database."""

    def add(self, pipeline: Pipeline) -> int:
        self._session.add(pipeline)
        self._session.flush()
        return pipeline.id

    def add_all(self, pipelines: List[Pipeline]) -> List[int]:
        ids = []
        for pipeline in pipelines:
            ids.append(self.add(pipeline))
        self._session.commit()
        return ids

    def get_by_id(self, pipeline_id: int) -> Pipeline:
        pipeline = self._session.query(Pipeline).filter(
            Pipeline.id == pipeline_id).all()
        return pipeline

    def get_by_name(self, pipeline_name: str) -> Pipeline:
        pipeline = self._session.query(Pipeline).filter(
            Pipeline.name == pipeline_name).all()
        return pipeline

    def get_all(self) -> list:
        return self._session.query(Pipeline).all()

    def delete(self, name: str) -> None:
        self._session.query(Pipeline).filter(
            Pipeline.name == name).delete()
        self._session.commit()

    def delete_all(self) -> None:
        self._session.query(Pipeline).delete()
        self._session.commit()


# --------------------------------------------------------------------------- #
#                            STEP REPOSITORY                                  #
# --------------------------------------------------------------------------- #
class StepRepo(Repository):
    """Database administration for the repository database."""

    def add(self, step: Step) -> int:
        self._session.add(step)
        self._session.flush()
        return step.id

    def add_all(self, steps: List[Step]) -> List[int]:
        ids = []
        for step in steps:
            ids.append(self.add(step))
        self._session.commit()
        return ids

    def get_by_id(self, step_id: int) -> Step:
        step = self._session.query(Step).filter(
            Step.id == step_id).all()
        return step

    def get_by_name(self, step_name: str) -> Step:
        step = self._session.query(Step).filter(
            Step.name == step_name).all()
        return step

    def get_all(self) -> list:
        return self._session.query(Step).all()

    def delete(self, name: str) -> None:
        self._session.query(Step).filter(
            Step.name == name).delete()
        self._session.commit()

    def delete_all(self) -> None:
        self._session.query(Step).delete()
        self._session.commit()
