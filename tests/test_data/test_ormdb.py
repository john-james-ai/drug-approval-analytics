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
# Modified : Wednesday, July 7th 2021, 12:57:31 am                             #
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
from src.data.datasources import DataSource, AACTDataSource, DrugsDataSource, LabelsDataSource
from src.database.ormdb import ORMDataSource
from configs.config import Credentials
from src.utils import files
# -----------------------------------------------------------------------------#

@pytest.mark.ormdb
class ORMDataSourceTests:

    def test_setup(self):
        """Instantiate a database to work with."""        
        credentials = Credentials('dara')
        self.db = ORMDataSource(credentials)
        configuration = AACTConfig()
        self.aact = AACTDataSource(configuration)
        configuration = DrugsConfig()
        self.drugs = DrugsDataSource(configuration)        
        configuration = LabelsConfig()
        self.labels = LabelsDataSource(configuration)

    def test_drop(self):
        self.db.drop()

    def test_tear_down(self):
        self.db.delete_all()
        self.db.close()

    def test_add(self):        
        self.db.add(self.aact)
        self.db.add(self.drugs)
        self.db.add(self.labels)


    def test_read(self):
        aact = self.db.read('aact')        
        drugs = self.db.read('drugs')
        labels = self.db.read('labels')

        for result in aact:
            assert result.name ==  'aact', "Error: The objects that were added and then read don't match"

        for result in drugs:
            assert result.name ==  'drugs', "Error: The objects that were added and then read don't match"

        for result in labels:
            assert result.name ==  'labels', "Error: The objects that were added and then read don't match"


    def test_read_all(self):
        ds = [source for source in self.db.read_all()]
        assert len(ds) == 3, "Error. Expected 3, but got {} objects".format(len(ds))
        assert isinstance(ds[0], DataSource), "Error: The returned type wasn't polymorphically a DataSource"
        assert isinstance(ds[1], DataSource), "Error: The returned type wasn't polymorphically a DataSource"
        assert isinstance(ds[2], DataSource), "Error: The returned type wasn't polymorphically a DataSource"

    def test_delete(self):
        # Not tested due to class hierarchy constrants
        self.db.delete('labels')
        ds = [source for source in self.db.read_all()]
        assert len(ds) == 2, "Error. The number of objects did not match expected results."

    def test_delete_all(self):
        self.db.delete_all()
        ds = [source for source in self.db.read_all()]
        assert len(ds) == 0, "Error. The number of objects did not match expected results."





def main():
    ormtest = ORMDataSourceTests()
    ormtest.test_setup()
    ormtest.test_tear_down()
    #ormtest.test_drop()
    ormtest.test_add()
    ormtest.test_read()
    ormtest.test_read_all()
    #ormtest.test_delete()
    ormtest.test_delete_all()    


if __name__ == "__main__":
    main()   

#%%