#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#==============================================================================#
# Project  : Drug Approval Analytics                                           #
# Version  : 0.1.0                                                             #
# File     : \src\data\explore.py                                              #
# Language : Python 3.9.5                                                      #
# -----------------------------------------------------------------------------#
# Author   : John James                                                        #
# Company  : nov8.ai                                                           #
# Email    : john.james@nov8.ai                                                #
# URL      : https://github.com/john-james-sf/drug-approval-analytics          #
# -----------------------------------------------------------------------------#
# Created  : Sunday, July 18th 2021, 1:21:23 pm                                #
# Modified : Tuesday, July 20th 2021, 6:17:56 am                               #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#

class Explorer:
    """Class supporting Exploratory Data Analysis."""

    def __init__(self, df):
        self._df = df
        self._profile = {}

    def profile(self):
        
        self._profile['num_observations'] = self._df.shape[0]
        self._profile['num_attributes'] = self._df.shape[1]
        self._profile['num_cells'] = self._df.shape[0] * self._df.shape[1]
        self._profile['datatypes'] = self._df.dtypes.to_frame().reset_index()
        self._profile['datatypes'].columns = ["Attribute", "Datatype"]

        self._profile['num_attributes_by_datatype'] = \
            self._profile['datatypes'].groupby(by='Datatype').count()
        
        self._profile['pct_attributes_by_datatype'] = \
            self._profile['datatypes'].groupby(by='Datatype').count() / \
            self._df.shape[0] * 100
        
        self._profile['num_missing_cells'] = self._df.isna().sum()
        
        self._profile['pct_missing_cells'] = self._df.isna().sum() / \
            self._profile['num_cells'] * 100
        
        self._profile['num_complete_observations'] = self._df.shape[0] - \
            self._df.isnull().any(axis=1).sum()
        
        self._profile['pct_complete_observations'] = \
            self._profile['num_complete_observations'] /\
            self._profile['num_observations'] * 100

        self._profile['num_complete_attributes'] = self._df.shape[1] - \
            self._df.isnull().any(axis=0).sum()
        
        self._profile['pct_complete_attributes'] = \
            self._profile['num_complete_attributes'] /\
            self._profile['num_attributes'] * 100

        self._profile['memory_usage'] = self._df.memory_usage(deep=True).sum()
        return self._profile
