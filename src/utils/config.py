#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#==============================================================================#
# Project  : Drug Approval Analytics                                           #
# Version  : 0.1.0                                                             #
# File     : \config\config.py                                                 #
# Language : Python 3.9.5                                                      #
# -----------------------------------------------------------------------------#
# Author   : John James                                                        #
# Company  : nov8.ai                                                           #
# Email    : john.james@nov8.ai                                                #
# URL      : https://github.com/john-james-sf/drug-approval-analytics          #
# -----------------------------------------------------------------------------#
# Created  : Tuesday, July 13th 2021, 10:41:45 pm                              #
# Modified : Sunday, July 18th 2021, 2:03:35 am                                #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#
import os
from configparser import ConfigParser
import logging
logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------#
class Config:
    """Access object to the configuration file."""

    filepath  = "config.cfg"    

    def __init__(self, filepath=None):
        self._filepath = filepath if filepath is not None else Config.filepath

    def _check_file(self, filepath):
        if not os.path.exists(filepath):
            logger.error("Configuration file {} not found.".format(filepath))
            raise FileNotFoundError

    def get_section(self, section):
        """Returns dictionary of key-value pairs for a designated configuration section."""        

        parser = ConfigParser()                                
        self._check_file(self._filepath)        
        parser.read(self._filepath)  
        
        config = {}        
        try:        
            params = parser.items(section)
        except Exception as e:
            logger.error(e)
            raise(e)

        for param in params:
            config[param[0]] = param[1]            
        return config

    def set_section(self, section, params):
        """Sets or creates a section of parameters to config file."""

        parser = ConfigParser()                                        
        parser.read(self._filepath)  

        if not parser.has_section(str(section)):
            parser.add_section(str(section))
            parser[str(section)] = {}

        for option, value in params.items():
            parser[str(section)][str(option)] = str(value)

        with open(self._filepath, 'w+') as configfile:
            parser.write(configfile)
        
    def get_config(self, section, option):
        """Returns a configuration value given a section and option."""             

        parser = ConfigParser()                                
        self._check_file(self._filepath)        
        parser.read(self._filepath)   

        try:
            config = parser.get(section, option)        
        except Exception as e:
            logger.error(e)
            raise(e)

        return  config

    def set_config(self, section, option, value):        
        """Sets an existing configuration."""
    
        parser = ConfigParser()                 
        parser.read(self._filepath)  

        if not parser.has_section(str(section)):
            parser.add_section(str(section))
            parser[str(section)] = {}                 
            
        parser[section][option] = value
        with open(self._filepath, 'w+') as configfile:
            parser.write(configfile)        

    @property
    def sections(self):
        """Returns a list of all sections in the configuration file."""
        parser = ConfigParser()                                
        parser.read(self._filepath)
        return parser.sections()            

# -----------------------------------------------------------------------------#
#                            CONFIG READER                                     #
# -----------------------------------------------------------------------------#        
class Credentials:
    """Access object to Config. This makes configurations read-only."""

    filepath = 'credentials.cfg'

    def __init__(self, filepath=None):        
        self._filepath = filepath if filepath is not None else Credentials.filepath    

    def __call__(self, dbname):
        dbnames = Config(self._filepath).get_config('database', 'names')
        if dbname not in dbnames:
            raise Exception("{} is not a valid dbname".format(dbname))

        self._credentials = Config(self._filepath).get_section(dbname)        
        return self._credentials

    @property
    def dbname(self):
        return self._credentials['dbname']

    @property
    def host(self):
        return self._credentials['host']        

    @property
    def user(self):
        return self._credentials['user']        

    @property
    def password(self):
        return self._credentials['password']        

    @property
    def port(self):
        return self._credentials['port']        

# -----------------------------------------------------------------------------#
#                            DATA SOURCES                                      #
# -----------------------------------------------------------------------------#
class DataSourcesConfig:
    """Access toi data source configurations."""

    filepath = 'config.cfg'

    def __init__(self, filepath=None):
        self._filepath = filepath if filepath is not None else DataSourcesConfig.filepath              

    def __call__(self, datasource):        
        sources = Config(self._filepath).get_config('data', 'sources')
        if datasource not in sources:
            raise Exception("{} is not a valid data source.".format(datasource))        

        config = Config(self._filepath).get_section(datasource)    
        for k,v in config.items():
            setattr(self, k,v)


    @property
    def datasources(self):
        sources = Config().get_config('data', 'sources')
        sources = sources.split(',')
        return sources