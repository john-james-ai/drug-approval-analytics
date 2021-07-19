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
# Modified : Monday, July 19th 2021, 1:26:10 pm                                #
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

    def has_section(self, section):
        parser = ConfigParser()                                        
        parser.read(self._filepath)  
        return parser.has_section(str(section))

    def has_option(self, section, option):
        parser = ConfigParser()                                        
        parser.read(self._filepath)  
        return parser.has_option(str(section), str(option))        


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
class DBConfig:
    """Access object to Config. This makes configurations read-only."""

    filepath = 'database.cfg'

    def __init__(self, filepath=None):        
        self._filepath = filepath if filepath is not None else DBConfig.filepath    

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

# -----------------------------------------------------------------------------#
#                            METADATA CONFIG                                   #
# -----------------------------------------------------------------------------#        
class MetadataElementTypeConfig:
    """Manages metadata element type configuration in terms of admin and access"""

    filepath = "../data/metadata/element.cfg"
    options = {}    

    def __init__(self, name):
        self._filepath = MetadataConfig.filepath
        self._name = name
        self._config = Config(self._filepath)
        self._admin_section = name + '_admin'
        self._access_section = name + '_access'    
    
    def get_admin_config(self):
        return self._config.get_section(self._admin_section)        

    def set_admin_config(self, option, value):
        self._config.set_option(self._admin_section, option, value)
    
    def get_access_section(self):
        return self._config.get_section(self._access_section)        

    def set_access_config(self, option, value):
        self._config.set_option(self._access_section, option, value)        

    # ----------------------------------------------------------------------- #
    #                            ADMIN CONFIG                                 #
    # ----------------------------------------------------------------------- #
    @property
    def create_table(self):
        return self._config.get_option(self._admin_section, 'create_table')
    
    @create_table.setter
    def create_table(self, command):
        self._config.set_option(self._admin_section, 'create_table', command)

    @property
    def add_property(self):
        return self._config.get_option(self._admin_section, 'add_property')
    
    @add_property.setter
    def add_property(self, command):
        self._config.set_option(self._admin_section, 'add_property', command)

    @property
    def drop_property(self):
        return self._config.get_option(self._admin_section, 'drop_property')
    
    @drop_property.setter
    def drop_property(self, command):
        self._config.set_option(self._admin_section, 'drop_property', command)        

    @property
    def drop_table(self):
        return self._config.get_option(self._admin_section, 'drop_table')
    
    @drop_table.setter
    def drop_table(self, command):
        self._config.set_option(self._admin_section, 'drop_table', command)

    @property
    def backup_table(self):
        return self._config.get_option(self._admin_section, 'backup_table')
    
    @backup_table.setter
    def backup_table(self, command):
        self._config.set_option(self._admin_section, 'backup_table', command)

    @property
    def restore_table(self):
        return self._config.get_option(self._admin_section, 'restore_table')
    
    @restore_table.setter
    def restore_table(self, command):
        self._config.set_option(self._admin_section, 'restore_table', command)

    # ----------------------------------------------------------------------- #
    #                            ACCESS CONFIG                                #
    # ----------------------------------------------------------------------- #
    @property
    def create_element(self):
        return self._config.get_option(self._admin_section, 'create_element')
    
    @create_element.setter
    def create_element(self, command):
        self._config.set_option(self._admin_section, 'create_element', command)

    @property
    def update_element(self):
        return self._config.get_option(self._admin_section, 'update_element')
    
    @update_element.setter
    def update_element(self, command):
        self._config.set_option(self._admin_section, 'update_element', command)       

    @property
    def read_element(self):
        return self._config.get_option(self._admin_section, 'read_element')
    
    @read_element.setter
    def read_element(self, command):
        self._config.set_option(self._admin_section, 'read_element', command)                

    @property
    def delete_element(self):
        return self._config.get_option(self._admin_section, 'delete_element')
    
    @delete_element.setter
    def delete_element(self, command):
        self._config.set_option(self._admin_section, 'delete_element', command)                        