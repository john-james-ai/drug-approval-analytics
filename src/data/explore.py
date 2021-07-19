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
# Modified : Sunday, July 18th 2021, 4:00:21 pm                                #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#
import os
import pandas as pd

class Explorer:
    """Class supporting Exploratory Data Analysis."""

    def __init__(self, entity, df):
        self.df = df
        

    def profile(self):
        n_observations = df.shape[0]
        n_attributes = df.shape[1]
        datatypes = df.dtypes.reset_index()
        print()
