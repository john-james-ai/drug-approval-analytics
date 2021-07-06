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
# Modified : Monday, July 5th 2021, 4:07:30 pm                                 #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#
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
# Modified : Monday, July 5th 2021, 3:59:41 pm                                 #
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

from configs.config import AACTConfig, DrugsConfig, LabelsConfig
from src.data.datasources import AACTDataSource, DrugsDataSource, LabelsDataSource
from src.data.database import DBDao
from src.utils import files
# -----------------------------------------------------------------------------#

@pytest.mark.datasources
class DataSourceTests:

    def test_aact_datasource(self):
        config = AACTConfig()
        dbdao = DBDao(config)
        downloaded_dir = config.persistence
        staged_dir = config.staged_dir
        ds = AACTDataSource(config, dbdao)
        ds.get()        
        assert files.numfiles(downloaded_dir) == 6, "AACTDataSource failed to download data to {}.".format(downloaded_dir)
        assert files.numfiles(staged_dir) > 50, "AACTDataSource failed to load staging area {}.".format(staged_dir)
        
        
    def test_drugs_datasource(self):
        config = DrugsConfig()        
        staged_dir = config.staged_dir
        downloaded_dir = config.persistence
        ds = DrugsDataSource(config)
        ds.get()
        # Confirm file is updated
        assert files.numfiles(downloaded_dir) == 6, "DrugsDataSource failed to download data to {}.".format(downloaded_dir)
        assert files.numfiles(staged_dir) > 50, "DrugsDataSource failed to load staging area {}.".format(staged_dir)
        

    def test_labels_datasource(self):
        config = DrugsConfig()        
        staged_dir = config.staged_dir
        downloaded_dir = config.persistence
        ds = LabelsDataSource(configuration)(config)
        ds.get()
        # Confirm file is updated
        assert files.numfiles(downloaded_dir) == 6, "LabelsDataSource failed to download data to {}.".format(downloaded_dir)
        assert files.numfiles(staged_dir) > 50, "LabelsDataSource failed to load staging area {}.".format(staged_dir)

dst = DataSourceTests()
dst.test_aact_datasource()
dst.test_drugs_datasource()()
dst.test_labels_datasource()()

# %%
