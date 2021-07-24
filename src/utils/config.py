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
# Modified : Saturday, July 24th 2021, 6:04:26 am                             #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
import os
import logging
from configparser import ConfigParser
import yaml
# --------------------------------------------------------------------------- #
logger = logging.getLogger(__name__)
# --------------------------------------------------------------------------- #


class Config:
    """Access object to the configuration file."""

    def __init__(self, filepath: str) -> None:
        self._filepath = filepath

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
#                     DATABASE CREDENTIALS READER                             #
# --------------------------------------------------------------------------- #


class DBCredentials:
    """Access object to for database credentials.

    Returns the database credentials for database having the dbname. Properties
    are exposed for credentials dictionary and for each variable
    independently.  The dictionary property is provided for
    database connection applications that accept credentials in
    a dictionary format.

    Arguments:
        filepath str: The path to the credentials configuration file.
    """

    filepath = './config/database.cfg'

    def __init__(self, filepath: str = None) -> None:
        self._filepath = filepath if filepath is not None else \
            DBCredentials.filepath
        self._credentials = {}

    def get_config(self, dbname: str) -> dict:
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

    @property
    def credentials(self):
        return self._credentials


# ----------------------------------------------------------------------------#
#                            DATA SOURCES                                     #
# ----------------------------------------------------------------------------#
class DataSourceConfig:
    """Access to data source configurations."""

    filepath = './config/datasources.cfg'

    def __init__(self, filepath: str = None) -> None:
        self._filepath = filepath if filepath is not None \
            else DataSourceConfig.filepath

    def get_config(self, name: str) -> dict:
        sources = Config(self._filepath).get_config('data', 'sources')
        if name not in sources:
            raise Exception("{} is not a valid data source.".format(name))

        return Config(self._filepath).get_section(name)

    @property
    def datasources(self) -> list:
        sources = Config().get_config('data', 'sources')
        sources = sources.split(',')
        return sources

# ----------------------------------------------------------------------------#
#                            JSON CONFIG PARSER                               #
# ----------------------------------------------------------------------------#


class RepositoryConfig:
    """Defines the schema for the Repository database."""

    filepath = './config/repository.yaml'

    def __init__(self, filepath: str = None) -> None:
        self._filepath = filepath if filepath is not None \
            else RepositoryConfig.filepath

    def get_config(self, name: str = None) -> dict:
        config = {}
        with open(self._filepath) as file:
            data = yaml.load(file, Loader=yaml.FullLoader)

        if name:
            try:
                config = data.get(name)
            except Exception as e:
                logger.error(e)
        else:
            config = data

        return config

    def set_config(self, config: dict) -> None:
        with open(self._filepath, 'w') as file:
            yaml.dump(config, file)


# ----------------------------------------------------------------------------#
#                           CONFIGURATIONS                                    #
# ----------------------------------------------------------------------------#
# Database credentials
dba_credentials = DBCredentials().get_config('postgres')
aact_credentials = DBCredentials().get_config('AACT')
daa_credentials = DBCredentials().get_config('daa')
# Database configurations
repository_config = RepositoryConfig().get_config()
