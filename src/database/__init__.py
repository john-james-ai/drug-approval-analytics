#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#==============================================================================#
# Project  : Predict-FDA                                                       #
# Version  : 0.1.0                                                             #
# File     : \__init__.py                                                      #
# Language : Python 3.9.5                                                      #
# -----------------------------------------------------------------------------#
# Author   : John James                                                        #
# Company  : nov8.ai                                                           #
# Email    : john.james@nov8.ai                                                #
# URL      : https://github.com/john-james-sf/predict-fda                      #
# -----------------------------------------------------------------------------#
# Created  : Tuesday, July 6th 2021, 2:53:38 am                                #
# Modified : Saturday, July 10th 2021, 8:11:46 am                              #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#
from abc import ABCMeta, abstractmethod
from sqlalchemy.ext.declarative import declarative_base, DeclarativeMeta
Base = declarative_base()

class DeclarativeABCMeta(DeclarativeMeta, ABCMeta):
    pass

AbstractBase = declarative_base(metaclass=DeclarativeABCMeta)
