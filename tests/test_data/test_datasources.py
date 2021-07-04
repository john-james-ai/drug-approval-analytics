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
# Modified : Sunday, July 4th 2021, 3:34:32 pm                                 #
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

from configs.config import Config
from src.data.datasources import PGDataSource, DrugsFDA, OpenFDA
# -----------------------------------------------------------------------------#

@pytest.mark.datasources
class DataSourceTests:

    def test_pg_database(self):
        filename = './data/external/aact/postgres_data.dmp'
        ds = PGDataSource('aact', Config())
        ds.get()
        file_updated = date.fromtimestamp(os.path.getmtime(filename))
        today = date.fromtimestamp(time.time())
        
    def test_drugsfds(self):
        filename = './data/external/drugsfda/TE.txt'
        ds = DrugsFDA('drugsfda', Config())
        ds.get()
        file_updated = date.fromtimestamp(os.path.getmtime(filename))
        today = date.fromtimestamp(time.time())
        assert today == file_updated

    def test_openfda(self):
        filename = './data/external/openfda_label/drug-label-0001-of-0010.json'
        ds = OpenFDA('openfda', Config())
        ds.get()
        file_updated = date.fromtimestamp(os.path.getmtime(filename))
        today = date.fromtimestamp(time.time())
        assert today == file_updated



# %%
