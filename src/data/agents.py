#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#==============================================================================#
# Project  : Predict-FDA                                                       #
# Version  : 0.1.0                                                             #
# File     : \agents.py                                                        #
# Language : Python 3.9.5                                                      #
# -----------------------------------------------------------------------------#
# Author   : John James                                                        #
# Company  : nov8.ai                                                           #
# Email    : john.james@nov8.ai                                                #
# URL      : https://github.com/john-james-sf/predict-fda                      #
# -----------------------------------------------------------------------------#
# Created  : Thursday, July 1st 2021, 2:00:59 pm                               #
# Modified : Sunday, July 4th 2021, 4:54:54 pm                                 #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#
from abc import ABC, abstractmethod

import pandas as pd
# -----------------------------------------------------------------------------#
class Agent(ABC):
    """Interface for agents i.e. data pipeline workers that act on the data."""
    def __init__(self):
        pass

    @abstractmethod
    def execute(self, dataobject):
        pass
# -----------------------------------------------------------------------------#
class Profiler(Agent):
    """Presents univariate analysis of a dataset."""
    def __init__(self):
        pass

    def _profile_base(self):
        self._data = self._dataobject.data
        self._profile['Observations'] = self._data.shape[0]
        self._profile['Attributes'] = self._data.shape[1]    
        self._profile['n_missing_column'] = pd.isna(self._data).sum() 
        self._profile['n_missing_total'] = pd.isna(self._data).sum().sum()   
        self._profile['Pct_missing_total'] = pd.isna(self._data).sum().sum() / (self._data.shape[0] * self._data.shape[1]) * 100        
        
        dtype_counts = self._data.dtypes.value_counts()
        self._profile['Datatypes'] = dtype_counts.index
        for datatype in dtype_counts.index:
            self._profile[datatype] = dtype_counts[datatype]

    def _profile_numeric(self):
        df = self._data.select_dtypes(include=['number'])
        if (df.shape[0] > 0):
            self._profile['Numeric'] = df.describe()

    def _profile_categorical(self):
        df = self._data.select_dtypes(include=['category', 'object'])
        if (df.shape[0] > 0):
            self._profile['Categorical'] = df.describe()

    def _profile_datetime(self):        
        df = self._data.select_dtypes(include=['datetime64'])
        if (df.shape[0] > 0):
            d2 = {}
            d2['Min'] = df.min()
            d2['Max'] = df.max().values
            self._profile['Datetime'] = pd.DataFrame(data=d2)

    def _profile_boolean(self):
        df = self._data.select_dtypes(include=['boolean'])
        self._profile['Boolean'] = df.describe()
    
    def execute(self, dataobject):
        self._data = dataobject.data
        self._profile_base()
        self._profile_numeric()
        self._profile_categorical()
        self._profile_datetime()
        self._profile_boolean()
    
    def get_profile(self):
        return self._profile







