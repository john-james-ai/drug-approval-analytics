#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#==============================================================================#
# Project  : Predict-FDA                                                       #
# Version  : 0.1.0                                                             #
# File     : \config.py                                                        #
# Language : Python 3.9.5                                                      #
# -----------------------------------------------------------------------------#
# Author   : John James                                                        #
# Company  : nov8.ai                                                           #
# Email    : john.james@nov8.ai                                                #
# URL      : https://github.com/john-james-sf/predict-fda                      #
# -----------------------------------------------------------------------------#
# Created  : Monday, June 21st 2021, 3:59:38 am                                #
# Modified : Monday, June 28th 2021, 12:51:41 am                               #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#
from abc import ABC, abstractmethod
from configparser import SafeConfigParser

class Config(ABC):
    """Base class for Configuration classes"""
    @abstractmethod
    def get_config(self, datasource):
        pass

class DataSourceConfig(Config):
    """Provides access to data source configurations """

    filename = "configs\datasources.ini"    

    def __init__(self):
        pass
        

    def get_config(self, datasource='aact'):
        # create a parser
        filename = DataSourceConfig.filename
        parser = SafeConfigParser()
        # read config file
        parser.read(filename)

        # Get configuration
        config = {}
        if parser.has_section(datasource):
            params = parser.items(datasource)
            for param in params:
                config[param[0]] = param[1]
        else:
            raise Exception('Section {0} not found in the {1} file'.format(datasource, filename))

        return config

class DirectoryConfig(Config):
    """Provides access to directory configurations"""

    filename = "configs\directories.ini"
    
    def __init__(self):
        pass

    def get_config(self, datasource='aact'):
        # create a parser
        filename = DirectoryConfig.filename
        parser = SafeConfigParser()
        # read config file
        parser.read(filename)

        # Get configuration
        config = {}
        if parser.has_section('data'):
            params = parser.items('data')
            for param in params:
                config[param[0]] = param[1]
        else:
            raise Exception('Section {0} not found in the {1} file'.format(datasource, filename))

        return config[datasource]        