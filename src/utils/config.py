#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# =========================================================================== #
# Project  : Drug Approval Analytics                                          #
# Version  : 0.1.0                                                            #
# File     : \src\utils\config.py                                             #
# Language : Python 3.9.5                                                     #
# --------------------------------------------------------------------------  #
# Author   : John James                                                       #
# Company  : nov8.ai                                                          #
# Email    : john.james@nov8.ai                                               #
# URL      : https://github.com/john-james-sf/drug-approval-analytics         #
# --------------------------------------------------------------------------  #
# Created  : Thursday, July 15th 2021, 5:47:58 pm                             #
# Modified : Wednesday, July 21st 2021, 1:52:32 pm                            #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #

import os
from configparser import ConfigParser
import logging
logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------#


class Config:
    """Access object to the configuration file."""

    filepath = "config.cfg"

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

    def get_section(self, section: str) -> dict:
        """Returns section key-value pairs."""

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

        return config

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

# --------------------------------------------------------------------------- #
#                        DATABASE CONFIG READER                               #
# --------------------------------------------------------------------------- #


class DBConfig:
    """Access object to Config. This makes configurations read-only.

    Returns the database credentials for database having the dbname. Properties
    are exposed for credentials dictionary and for each variable
    independently.  The dictionary property is provided for
    database connection applications that accept credentials in
    a dictionary format.

    Arguments:
        dbname str: The 'dbname' credential for the database
        filepath str: Used for testing.
    """

    filepath = 'database.cfg'

    def __init__(self, dbname: str, filepath: str = None) -> None:
        self._dbname = dbname
        self._filepath = filepath
        self._credentials = self._get_config()

    def _get_config(self):
        if self._filepath is None:
            self._filepath = DBConfig.filepath

        dbnames = Config(self._filepath).get_config('database', 'names')
        if self._dbname not in dbnames:
            raise Exception("{} is not a valid dbname".format(self._dbname))

        return Config(self._filepath).get_section(self._dbname)

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

    @property
    def credentials(self):
        return self._credentials


# --------------------------------------------------------------------------- #
# Database configurations
pipeline_config = DBConfig('repository')
aact_config =  DBConfig('aact')
# ----------------------------------------------------------------------------#
#                            DATA SOURCES                                     #
# ----------------------------------------------------------------------------#


class DataSourcesConfig:
    """Access to data source configurations."""

    filepath = 'config.cfg'

    def __init__(self, filepath=None):
        self._filepath = filepath if filepath is not None \
            else DataSourcesConfig.filepath

    def __call__(self, name: str) -> dict:
        sources = Config(self._filepath).get_config('data', 'sources')
        if name not in sources:
            raise Exception("{} is not a valid data source.".format(name))

        return Config(self._filepath).get_section(name)

    @property
    def datasources(self):
        sources = Config().get_config('data', 'sources')
        sources = sources.split(',')
        return sources
