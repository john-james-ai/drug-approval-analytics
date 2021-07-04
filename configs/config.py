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
# Modified : Thursday, July 1st 2021, 11:51:07 pm                              #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#
from abc import ABC, abstractmethod
from configparser import SafeConfigParser

class Config():
    """Provides access to data source configurations """

    filename = "configs\config.ini"    

    def __init__(self):
        pass
        
    def set(self, section, option, value):
        filename = Config.filename
        parser = SafeConfigParser()
        parser.read(filename)        
        parser.set(section=section, option=option, value=value)
        with open(filename, 'w') as configfile:
            parser.write(configfile)

    def get(self, section, option=None):
        # create a parser
        filename = Config.filename
        parser = SafeConfigParser()
        # read config file
        parser.read(filename)

        config = {}

        if parser.has_section(section):

            if option is not None:
                config = parser.get(section=section, option=option)
            else:                
                params = parser.items(section)
                for param in params:
                    config[param[0]] = param[1]
        else:
            raise Exception('Section {0} not found in the {1} file'.format(section, filename))
            
        return config

