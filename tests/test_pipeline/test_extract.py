#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#==============================================================================#
# Project  : Predict-FDA                                                       #
# Version  : 0.1.0                                                             #
# File     : \test_extract_pipeline.py                                         #
# Language : Python 3.9.5                                                      #
# -----------------------------------------------------------------------------#
# Author   : John James                                                        #
# Company  : nov8.ai                                                           #
# Email    : john.james@nov8.ai                                                #
# URL      : https://github.com/john-james-sf/predict-fda                      #
# -----------------------------------------------------------------------------#
# Created  : Saturday, July 10th 2021, 10:30:07 am                             #
# Modified : Monday, July 12th 2021, 3:26:55 am                                #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#
#%%
import os
import pytest
from pytest import mark
from datetime import datetime, date

from config.config import Credentials, AACTConfig, DrugsConfig, LabelsConfig
from approval.database.postgres import DBDao
from approval.database.orm import DataSourcesFactory, DataObjectsFactory
from approval.pipeline.core import Pipeline, PipelineStep
from approval.data.datasources import AACTDataSource, DrugsDataSource, LabelsDataSource
from approval.pipeline.steps import Extract, Stage, StagePGDatabase, StageTSVFiles, StageJSONFiles
from approval.utils.files import modified_today
# -----------------------------------------------------------------------------#
@mark.pipeline
class ExtractPipelineTests:

    def __init__(self):   
        """Create and load datasources."""             
        self.datasources = DataSourcesFactory(dbname='test', empty=True).create()            
        self._load_datasources()

    def _load_datasources(self):        
        aact = AACTDataSource(AACTConfig())       
        drugs = DrugsDataSource(DrugsConfig())
        labels = LabelsDataSource(LabelsConfig())
        self.datasources.add(aact)
        self.datasources.add(drugs)
        self.datasources.add(labels)

    def test_extract(self):
        sources = self.datasources.read_all()
        for source in sources:
            pipeline = Pipeline(source.name, source)
            step = Extract('aact', 1)
            pipeline.add_step(step)            
            refreshable =  source.is_refreshable()
            source = pipeline.execute()
            if refreshable:            
                assert source.last_executed.date() == datetime.today().date(), "Error: Expired source {source} was not extracted".format(source=source.name)
                assert source.updated.date() == datetime.today().date(), "Error: Expired source {source} was not extracted".format(source=source.name)
                assert not source.expired, "Error source was expired after extract."            

    def test_stage(self):
        dao = DBDao()
        
        sources = [self.datasources.read('drugs')[0],
                   self.datasources.read('labels')[0]]
        stagers = {}
        stagers['aact'] = StagePGDatabase('aact', 2, dao)
        stagers['drugs'] = StageTSVFiles('drugs', 2)
        stagers['labels'] = StageJSONFiles('labels', 2)
        for source in sources:
            pipeline = Pipeline(source.name, source)
            step = stagers[source.name]
            pipeline.add_step(step)
            source = pipeline.execute()
            assert len(os.listdir(source.staging_dir)) > 0, "Error: Staging directory is empty for {}".format(source.name)
            

            

def main():    
    tests = ExtractPipelineTests()                            
    tests.test_stage()

if __name__ == '__main__':
    main()
#%%    