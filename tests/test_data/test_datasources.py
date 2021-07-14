#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#==============================================================================#
# Project  : Predict-FDA                                                       #
# Version  : 0.1.0                                                             #
# File     : \test_datasources.py                                              #
# Language : Python 3.9.5                                                      #
# -----------------------------------------------------------------------------#
# Author   : John James                                                        #
# Company  : nov8.ai                                                           #
# Email    : john.james@nov8.ai                                                #
# URL      : https://github.com/john-james-sf/predict-fda                      #
# -----------------------------------------------------------------------------#
# Created  : Friday, July 2nd 2021, 4:40:41 am                                 #
# Modified : Friday, July 9th 2021, 6:39:44 pm                                 #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#
#%%
from datetime import date
import pytest
import time
import os

from config.config import AACTConfig, DrugsConfig, LabelsConfig
from approval.data.datasources import AACTDataSource, DrugsDataSource, LabelsDataSource
from approval.database.postgres import DBDao
from approval.utils import files
# -----------------------------------------------------------------------------#

@pytest.mark.datasources
class DataSourceTests:

    def test_aact_datasource(self):
        config = AACTConfig()        
        downloaded_dir = config.persistence        
        ds = AACTDataSource(config)
        ds.get()        
        if ds.files_extracted:
            assert files.numfiles(downloaded_dir) == 7, "AACTDataSource failed to download data to {}.".format(downloaded_dir)        
        
        
    def test_drugs_datasource(self):
        config = DrugsConfig()        
        downloaded_dir = config.persistence
        ds = DrugsDataSource(config)
        ds.get()
        ds.summary()
        filenames = os.listdir(downloaded_dir)
        # Confirm file is updated
        if ds.files_extracted:            
            for filename in filenames:
                filepath = os.path.join(downloaded_dir, filename)
                assert files.modified_today(filepath), "DrugsDataSource error. {} was not downloaded today.".format(filename)
        

    def test_labels_datasource(self):
        config = LabelsConfig()        
        downloaded_dir = config.persistence
        ds = LabelsDataSource(config)
        ds.get()
        ds.summary()
        filenames = os.listdir(downloaded_dir)
        # Confirm file is updated
        if ds.files_extracted:
            assert files.numfiles(downloaded_dir) == 10, "LabelsDataSource failed to download data to {}.".format(downloaded_dir)   
            for filename in filenames:
                filepath = os.path.join(downloaded_dir, filename)
                assert files.modified_today(filepath)     

def main():
    dst = DataSourceTests()
    #dst.test_aact_datasource()
    #dst.test_drugs_datasource()
    #dst.test_labels_datasource()


if __name__ == "__main__":
    main()   



# %%
