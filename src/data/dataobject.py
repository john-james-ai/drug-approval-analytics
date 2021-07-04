#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#==============================================================================#
# Project  : Predict-FDA                                                       #
# Version  : 0.1.0                                                             #
# File     : \dataobject.py                                                    #
# Language : Python 3.9.5                                                      #
# -----------------------------------------------------------------------------#
# Author   : John James                                                        #
# Company  : nov8.ai                                                           #
# Email    : john.james@nov8.ai                                                #
# URL      : https://github.com/john-james-sf/predict-fda                      #
# -----------------------------------------------------------------------------#
# Created  : Thursday, July 1st 2021, 11:55:27 am                              #
# Modified : Sunday, July 4th 2021, 4:57:31 pm                                 #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#
from abc import ABC, abstractmethod
from datetime import date, datetime

import pandas as pd

# -----------------------------------------------------------------------------#
class DataObject(ABC):
    """Definitional unit of data."""
    def __init__(self, source, name, **kwargs):
        self.source = source
        self.name = name
        self._data = None    
        self._profiles = {}     

        self._state = 'Created'
        self._state_datetime = datetime.now()        

    def get_data(self):
        return self._data 

    def set_data(self, x):
        self._data = x



        