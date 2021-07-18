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
# Modified : Sunday, July 18th 2021, 4:42:30 am                                #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#
#%%
from datetime import datetime
import pytest
import time
import os

from src.data.meta.dbao import DataSources
import logging
logger = logging.getLogger(__name__)
# -----------------------------------------------------------------------------#

@pytest.mark.metadata
class DataSourceTests:

    def __init__(self):
        self.datasources = DataSources()


    def test_load(self):
        self.datasources.load()     
        datasources = self.datasources.list()
        assert len(datasources) == 8, "Error: The number of DataSource objects not loaded != 8"           

    def test_get(self):
        names = self.datasources.list()
        datasources = self.datasources.get()
        assert len(datasources) == 8, "Error: The number of DataSource objects not loaded != 8"           
        for datasource in datasources:
            assert datasource.name in names, "Error: DataSource name {} not valid".format(datasource.name)

    def test_update(self):
        today = datetime.now()
        names = self.datasources.list()
        for name in names:
            self.datasources.update_last_extracted(name, today)
            self.datasources.update_last_updated(name, today)
            self.datasources.update_last_staged(name, today)
        datasources = self.datasources.get()
        for datasource in datasources:
            assert datasource.last_updated == today, "Error: DataSource last updated not valid"
            assert datasource.last_extracted == today, "Error: DataSource last updated not valid"
            assert datasource.last_staged == today, "Error: DataSource last updated not valid"

    def test_copy(self):
        ds1 = self.datasources.get('labels')        
        ds2 = self.datasources.copy(ds1)    
        assert ds1 == ds2, "Error: Copy didn't work \n{}\n{}".format(ds1, ds2)

        
    def test_delete(self):
        all_sources = self.datasources.list()
        drugs = self.datasources.get('drugs')
        self.drugs_copy = self.datasources.copy(drugs)
        self.datasources.delete('drugs')
        sources = self.datasources.list()
        assert (len(all_sources) == len(sources) +1 ), "Error: Delete didn't work. Sources before = {}, after = {}".format(len(all_sources), len(sources))

    def test_add(self):
        self.datasources.add(self.drugs_copy)
        names = self.datasources.list()
        assert (len(names)) == 8, "Error: Add didn't work"
        print("DataSourceTests completed!")

        
def main():    
    dst = DataSourceTests()
    dst.test_load()
    dst.test_get()    
    dst.test_update()
    dst.test_copy()
    dst.test_delete()
    dst.test_add()

if __name__ == "__main__":
    main()   



# %%
