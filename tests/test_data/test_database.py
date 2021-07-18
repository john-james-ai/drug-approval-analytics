#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#==============================================================================#
# Project  : Predict-FDA                                                       #
# Version  : 0.1.0                                                             #
# File     : \test_database.py                                                 #
# Language : Python 3.9.5                                                      #
# -----------------------------------------------------------------------------#
# Author   : John James                                                        #
# Company  : nov8.ai                                                           #
# Email    : john.james@nov8.ai                                                #
# URL      : https://github.com/john-james-sf/predict-fda                      #
# -----------------------------------------------------------------------------#
# Created  : Sunday, July 4th 2021, 6:46:35 pm                                 #
# Modified : Friday, July 16th 2021, 3:06:13 am                                #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#
#%%
import pytest

from src.data.database import DBAdmin
# -----------------------------------------------------------------------------#

@pytest.mark.database
class DBAdminTests:

    def test_create_datebase(self):
        
        dba = DBAdmin()
        dbname = 'test'
        db = dba.create_database(dbname)
        assert db == dbname, "Error: Database creation was not successful"        

@pytest.mark.dbdao
class DBDaoTests:
    def test_read_table(self):
        aact_config = AACTConfig('aact')
        db = DBDao(aact_config)
        data = db.read_table('studies')
        assert data.shape[0] > 1000, "Error reading 'studies' table"


    def test_get_columns(self):
        aact_config = AACTConfig('aact')
        db = DBDao(aact_config)
        tables = db.tables        
        columns = db.get_columns("studies")        
        assert len(tables) > 50, "Error reading tables from DBAdmin" 
        assert columns.shape[0] > 0, "Error reading columns from DBAdmin"
        assert columns.shape[1] > 0, "Error reading columns from DBAdmin"    

def main():
    dbt = DBAdminTests()
    dbt.test_create_datebase()


if __name__ == "__main__":
    main()   

