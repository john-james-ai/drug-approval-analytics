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
# Modified : Sunday, July 11th 2021, 6:21:03 pm                                #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#
import os
from configparser import ConfigParser
from ..drug-approval.


# -----------------------------------------------------------------------------#
class Config:
    """Interface for Python ConfigParser configurations."""

    filepath = {}
    filepath['dev'] = "../drug-approval/config/metadata.ini"    
    filepath['prod'] = "./config/metadata.ini"    

    def __init__(self, env='dev'):
        self._env = env
        self._filepath = MetaDataConfig.filepath[env]



    def get(self, section, option=None):
        """Obtains the configuration value from teh requested file and section."""

        config = {}        

        try:
            parser = ConfigParser()                                
            parser.read(self._filepath)



        if not os.path.exists(self._filepath):
            raise FileNotFoundError(self._filepath)        

        if parser.has_section(section):

        


        
            if option is not None:
                config = parser[section][option]
            else:                
                params = parser.items(section)
                for param in params:
                    config[param[0]] = param[1]
        else:
            raise Exception('Section {0} not found in the {1} file'.format(section, self._filepath))
        return config
        
        
    def set(self, section, option, value):        
    
        parser = ConfigParser()       
        parser[section][option] = value
        with open(self._filepath, 'w') as configfile:
            parser.write(configfile)

# -----------------------------------------------------------------------------#
class Credentials:
    """Obtains credential information

class Credentials:
    """Class used to obtain database credentials."""
    def __init__(self, name):
        self.name = name
        credentials = Config().get(name + '_credentials')
        self._database = credentials['database']
        self._host = credentials['host']
        self._user = credentials['user']
        self._password = credentials['password']
        self._port = credentials['port']

    @property
    def database(self):
        return self._database

    @property
    def host(self):
        return self._host

    @property
    def user(self):
        return self._user

    @property
    def password(self):
        return self._password

    @property
    def port(self):
        return self._port
# -----------------------------------------------------------------------------#
class Configuration(ABC):

    def __init__(self, name):
        self.name = name

    @property
    def webpage(self):
        return Config().get(self.name, 'webpage')

    @property
    def baseurl(self):
        return Config().get(self.name, 'baseurl')

    @property
    def extract_dir(self):
        return Config().get(self.name, 'extract_dir')     

    @property
    def staging_dir(self):
        return Config().get(self.name, 'staging_dir')             

    @property
    def backup_filepath(self):
        return Config().get(self.name, 'backup_filepath')

    @property
    def lifecycle(self):
        return int(Config().get(self.name, 'lifecycle'))

        
# -----------------------------------------------------------------------------#
class AACTConfig(Configuration):

    def __init__(self, name='aact'):
        self.name = name

    @property
    def schema_name(self):
        return Config().get(self.name, 'schema_name')       

    @property
    def backup_filepath(self):
        return Config().get(self.name, 'backup_filepath')

    @property
    def restore_filepath(self):
        return Config().get(self.name, 'restore_filepath')
        
    @property
    def backup_cmd(self):
        return Config().get(self.name, 'backup_cmd')

    @property
    def restore_cmd(self):
        return Config().get(self.name, 'restore_cmd')

    @property
    def credentials(self):
        return Config().get('aact_credentials')

# -----------------------------------------------------------------------------#
class LabelsConfig(Configuration):

    def __init__(self, name='labels'):
        self.name = name

    @property
    def download_links(self):
        return Config().get(self.name, 'download_links')


# -----------------------------------------------------------------------------#
class DrugsConfig(Configuration):

    def __init__(self, name='drugs'):
        self.name = name        

    @property
    def find(self):
        return Config().get(self.name, 'find')        

