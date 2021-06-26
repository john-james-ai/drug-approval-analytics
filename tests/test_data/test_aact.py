#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#==============================================================================#
# Project  : Predict-FDA                                                       #
# Version  : 0.1.0                                                             #
# File     : \test_aact.py                                                     #
# Language : Python 3.9.5                                                      #
# -----------------------------------------------------------------------------#
# Author   : John James                                                        #
# Company  : nov8.ai                                                           #
# Email    : john.james@nov8.ai                                                #
# URL      : https://github.com/john-james-sf/predict-fda                      #
# -----------------------------------------------------------------------------#
# Created  : Saturday, June 26th 2021, 1:48:44 pm                              #
# Modified : Saturday, June 26th 2021, 4:58:39 pm                              #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#
import pytest

from src.database.sqlgen import Queryator
from src.database.aact_db import AACTDao

class AACTTests:

    def test_get_column_names(self):
        table = "studies"
        aact = AACTDao()
        query_generator = Queryator()  
        query = query_generator.columns(table)        
        df = aact.read_query(query)
        assert df.size > 0
        




