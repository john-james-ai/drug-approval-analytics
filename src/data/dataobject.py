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
# Modified : Sunday, July 4th 2021, 11:22:55 pm                                #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#
import os
from datetime import date, datetime

import pandas as pd

# -----------------------------------------------------------------------------#
class DataObject:
    """Standard object for interacting with data in csv format.

    Parameters
    ----------
    name : str
        The name of the data object. Should be the basename for the file.
    configuration: dict
        Dict containing extract and transform directory information  
    """    
    
    def __init__(self, name, configuration):
        self.name = name 
        self._configuration = configuration
        self.persistence = configuration.persistence
        self.extract_dir = configuration.extract_dir
        self.transformed_dir = configuration.transformed_dir

    def read(self, directory):
        filepath = os.path.join(directory, self.name +'.csv')
        return pd.read_csv(filepath)

    def write(self, data, directory):
        filepath = os.path.join(directory, self.name +'.csv')
        data.to_csv(filepath)
