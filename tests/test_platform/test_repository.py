#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# =========================================================================== #
# Project  : Drug Approval Analytics                                          #
# Version  : 0.1.0                                                            #
# File     : \tests\test_operations\test_core.py                              #
# Language : Python 3.9.5                                                     #
# --------------------------------------------------------------------------  #
# Author   : John James                                                       #
# Company  : nov8.ai                                                          #
# Email    : john.james@nov8.ai                                               #
# URL      : https://github.com/john-james-sf/drug-approval-analytics         #
# --------------------------------------------------------------------------  #
# Created  : Thursday, July 29th 2021, 10:49:02 am                            #
# Modified : Friday, July 30th 2021, 1:01:22 am                               #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
# %%
from datetime import datetime

import pytest
import pandas as pd

from sqlalchemy import inspect

from src.operations.core import Artifact, DataSource, Entity, Feature
from src.operations.core import Executable, Event, Pipeline, Step
from src.operations.repository import ArtifactRepo, DataSourceRepo, EntityRepo, FeatureRepo
from src.operations.repository import ExecutableRepo, EventRepo, PipelineRepo, StepRepo
from src.operations.database import ORMDatabase
from src.operations.config import dba_credentials
# --------------------------------------------------------------------------- #


@pytest.mark.repository
class DataSourceTests:

    def __init__(self):
        # Get clean dataabase
        self.ormdb = ORMDatabase(dba_credentials)
        self.ormdb.reset()
        self.repo = ArtifactRepo(self.ormdb)

    def test_add(self):
        filepath = "./data/metadata/datasources.csv"
        df = pd.read_csv(filepath, index_col=0)
        sources = df.to_dict(orient='index')
        artifact_ids = []
        for name, details in sources.items():
            artifact = DataSource(name)
            artifact.description = details['description']
            artifact.creator = details['creator']
            artifact.webpage = details['webpage']
            artifact.uri = details['uri']
            artifact.uri_kind = details['uri_kind']
            artifact.media_type = details['media_type']
            artifact.frequency = details['frequency']
            artifact.lifecycle = details['lifecycle']
            artifact.updated = datetime.strptime(
                details['last_updated'], "%m,%d,%y")

            artifact_ids.append(self.repo.add(artifact))
        print(DataSource.__table__)
        artifacts = self.repo.get_all()
        print(artifacts)
        assert len(artifacts) == 10, "Add DataSource Error: Expected 10 \
            artifacts. Read {}".format(len(artifacts))
        for artifact_id in artifact_ids:
            artifact = self.repo.get_by_id(artifact_id)
            assert artifact.id == artifact_id, "Add DataSource Error: Expected \
                artifact_id {}. Observed {}".format(artifact_id, artifact.id)


def main():
    t = DataSourceTests()
    t.test_add()


if __name__ == '__main__':
    main()
    # %%
