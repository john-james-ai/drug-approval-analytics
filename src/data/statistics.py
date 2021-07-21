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
# Modified : Tuesday, July 20th 2021, 12:55:03 pm                              #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#
from abc import ABC, abstractmethod
# -----------------------------------------------------------------------------#
#                                                                              #
# -----------------------------------------------------------------------------#

class Statistics(ABC):
    """Abstraction for classes that compute and store feature statistics."""

    @abstractmethod
    def compute(self):
        pass

    @abstractmethod
    def plot(self):
        pass
    
    @abstractmethod
    def save(self):
        pass

class Descriptive(Statistics):
    """Computes frequency, centrality, variation and position statistics.
    
    Arguments
    ---------
    df (DataFrame): DataFrame of feature or feature group data

    Attributes
    ----------


    """

    def __init__(self, feature):
        self._feature = feature

    def compute(self):

    
    
# -----------------------------------------------------------------------------#
#                   FEATURE GROUP (MULTIVARIATE) STATISTICS                    #
# -----------------------------------------------------------------------------#
class FeatureGroupStatistics(Statistics):

    def __init__(self, feature_group):
        self._feature_group = feature_group

    

