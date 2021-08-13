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
# Modified : Friday, August 13th 2021, 7:42:31 am                             #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
import os
import logging
from configparser import ConfigParser
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

    def delete_section(self, section: str):
        """Removes a section from the config file."""

        parser = ConfigParser()
        parser.read(self._filepath)
        parser.remove_section(section)
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

    filepath = 'config/database.cfg'

    def __init__(self, filepath: str = None) -> None:
        self._filepath = filepath if filepath is not None else \
            DBCredentials.filepath

    def keys(self):
        return list(self._credentials.keys())

    def __getitem__(self, key):
        return self._credentials[key]

    def create(self, name: str, user: str, password: str,
               host: str = 'localhost', dbname: str = 'rx2m',
               port: int = 5432) -> None:
        config = Config(self._filepath)
        section = name
        params = {}
        params['user'] = user
        params['password'] = password
        params['host'] = host
        params['dbname'] = dbname
        params['port'] = port
        config.set_section(section=section, params=params)

    def read(self, name: str) -> dict:
        self.load(name)
        return self._credentials

    def delete(self, name: str) -> None:
        Config(self._filepath).delete_section(name)

    def load(self, name: str):
        self._credentials = Config(self._filepath).get_section(name)
        return self

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


# ----------------------------------------------------------------------------#
#                            DATA SOURCES                                     #
# ----------------------------------------------------------------------------#
class DataSourceConfig:
    """Access to data source configurations."""

    filepath = '../config/datasources.cfg'

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
#                           CONFIGURATIONS                                    #
# ----------------------------------------------------------------------------#
# Database credentials
pg_login = DBCredentials().load('postgres')
rx2m_login = DBCredentials().load('rx2m')
rx2m_test_login = DBCredentials().load('rx2m_test')
aact_login = DBCredentials().load('AACT')
