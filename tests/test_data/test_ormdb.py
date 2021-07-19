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
# Modified : Monday, July 19th 2021, 3:26:56 am                                #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#
#%%
from datetime import date, datetime
import pytest
import time
import os

from config.config import AACTConfig, DrugsConfig, LabelsConfig
from approval.data.datasources import DataSource, AACTDataSource, DrugsDataSource, LabelsDataSource
from approval.data.dataobjects import DataObject
from approval.database.orm import DataSourcesFactory, DataObjectsFactory
from approval.database.orm import ORMDatabase, ORMDataSource, ORMDataObject
from config.config import DBConfig
from approval.utils import files
# -----------------------------------------------------------------------------#

@pytest.mark.orm
class ORMDataSourceTests:

    def __init__(self, datasources):
        self.datasources = datasources
        self.aact = AACTDataSource(AACTConfig())
        self.drugs = DrugsDataSource(DrugsConfig())
        self.labels = LabelsDataSource(LabelsConfig())
        self.datasources.delete_all()

    def test_add_read(self):
        self.datasources.add(self.aact)
        self.datasources.add(self.drugs)
        self.datasources.add(self.labels)

        aact = self.datasources.read(self.aact.name)[0]
        assert aact == self.aact, "Error: Add read error."
        drugs = self.datasources.read(self.drugs.name)[0]
        assert drugs == self.drugs, "Error: Add read error."
        labels = self.datasources.read(self.labels.name)[0]
        assert labels == self.labels, "Error: Add read error."        

    def test_update_read(self):
        last_extracted = datetime.now()

        self.aact.last_extracted = last_extracted        
        self.datasources.update(self.aact, 'last_extracted')
        aact = self.datasources.read('aact')[0]
        assert aact.last_extracted.date() == datetime.now().date(), "Error: Update Read of AACT"        
        
        self.drugs.last_extracted = last_extracted        
        self.datasources.update(self.drugs, 'last_extracted')
        drugs = self.datasources.read('drugs')[0]
        assert drugs.last_extracted.date() == datetime.now().date(), "Error: Update Read of Drugs"        

        self.labels.last_extracted = last_extracted        
        self.datasources.update(self.labels, 'last_extracted')
        labels = self.datasources.read('labels')[0]
        assert labels.last_extracted.date() == datetime.now().date(), "Error: Update Read of Labels"        

    def test_delete_exists(self):
        self.datasources.delete('aact')
        assert self.datasources.exists('aact') is False, "Error: Delete Exists"

    def test_read_all_delete_all(self):
        self.datasources.delete_all()
        datasources = self.datasources.read_all()        
        assert len(datasources) == 0, "Error read_all, delete_all"

@pytest.mark.orm
class ORMDataObjectTests:

    def __init__(self, dataobjects):
        testdatadir = "./tests/data/"
        testdatafile = "./tests/data/test.jpg"
        self.dataobjects = dataobjects
        self.file = FileDataObject(name='test_file', 
                                    description='Test FileDataObject',
                                    persistence=testdatafile)
        self.directory = DirectoryDataObject(name='test_dir', 
                                    description='Test DirectoryDataObject',
                                    persistence=testdatadir)                                    
        self.dataobjects.delete_all()

    def test_add_read(self):
        self.dataobjects.add(self.file)
        self.dataobjects.add(self.directory)        

        file_dataobject = self.dataobjects.read(self.file.name)[0]
        assert file_dataobject == self.file, "Error: Add read error."
        directory_dataobject = self.dataobjects.read(self.directory.name)[0]
        assert directory_dataobject == self.directory, "Error: Add read error."

    def test_delete_exists(self):
        self.dataobjects.delete('aact')
        assert self.dataobjects.exists('aact') is False, "Error: Delete Exists"

    def test_read_all_delete_all(self):
        self.dataobjects.delete_all()
        dataobjects = self.dataobjects.read_all()        
        assert len(dataobjects) == 0, "Error read_all, delete_all"

def main():
    # Instantiate ORM Table
    datasources = DataSourcesFactory().create()

    # Instantiate the Test Object
    test = ORMDataSourceTests(datasources)
    test.test_add_read()
    test.test_update_read()    
    #test.test_delete_exists()
    test.test_read_all_delete_all()  
    print("="*40)
    print("         DataSource ORM Tested")
    print("="*40)

    # Instantiate DataObjects Table
    dataobjects = DataObjectsFactory().create()

    # Instantiate the Test Object
    test = ORMDataObjectTests(dataobjects)
    test.test_add_read()
    #test.test_delete_exists()
    test.test_read_all_delete_all()      
    print("="*40)
    print("         DataObject ORM Tested")
    print("="*40)

    

if __name__ == "__main__":
    main()   

#%%