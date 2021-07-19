#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#==============================================================================#
# Project  : Drug Approval Analytics                                           #
# Version  : 0.1.0                                                             #
# File     : \tests\test_setup\test_config.py                                  #
# Language : Python 3.9.5                                                      #
# -----------------------------------------------------------------------------#
# Author   : John James                                                        #
# Company  : nov8.ai                                                           #
# Email    : john.james@nov8.ai                                                #
# URL      : https://github.com/john-james-sf/drug-approval-analytics          #
# -----------------------------------------------------------------------------#
# Created  : Thursday, July 15th 2021, 10:02:18 pm                             #
# Modified : Monday, July 19th 2021, 3:26:56 am                                #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#
#%%
import os
import pytest
from configparser import ConfigParser

from src.utils.config import Config, DBConfig, DataSourcesConfig

@pytest.mark.setup
class ConfigTests:

    filepath = "./tests/test_setup/config.cfg"

    def __init__(self):
        if os.path.exists(ConfigTests.filepath):
            os.remove(ConfigTests.filepath)

    def _read_config(self, filepath):
        config = ConfigParser()
        config.read(filepath)
        result = {}
        sections = config.sections()
        for section in sections:
            result[section] = {}
            for option in config[section]:
                result[section][option] = config[section][option]
        return result       


    def test_set_config(self):
        config = Config(ConfigTests.filepath)        
        section = 'new_section'
        option = 'some_option'
        value = 'some_value'
        params = {}
        params[section] = {}
        params[section][option] = value
        config.set_config(section, option, value)
        result = self._read_config(ConfigTests.filepath)
        assert result == params, "Error: expected = {}\n      actual = {}".format(params, result)


    def test_get_config(self):
        filepath = "./bogus"

        config = Config(filepath)        
        with pytest.raises(FileNotFoundError):
            config.get_config('section', 'option')

        config = Config(ConfigTests.filepath)        
        section = 'new_section'
        option = 'bogus'
        with pytest.raises(Exception):
            config.get_config(section, option)

        option = 'some_option'
        value = 'some_value'
        result = config.get_config(section, option)
        assert result == value, "Error: expected {}, but actual result was {}".format(value, result)

    def test_set_section_new(self):
        config = Config(ConfigTests.filepath)  
        section = 'another_new_section'
        option1 = 'another_new_option1'
        option2 = 'another_new_option2'
        value1 = 'another_new_value1'
        value2 = 'another_new_value2'        
        params = {}
        params[option1] = value1
        params[option2] = value2
        config.set_section(section, params)
        result = self._read_config(ConfigTests.filepath)
        assert section in result.keys(), "Error: section {} not found in {}.".format(section, result.keys()) 
        assert option1 in result[section].keys(), "Error: option {} not found in {}.".format(option1, result[section].keys()) 
        assert option2 in result[section].keys(), "Error: option {} not found in {section}.".format(option2, result[section].keys()) 
        assert value1 == result[section][option1], "Error: value {} not found in option {} of section {}".format(value1, option1, section)
        assert value2 == result[section][option2], "Error: value {} not found in option {} of section {}".format(value2, option2, section)

    def test_set_section_existing(self):
        config = Config(ConfigTests.filepath)                
        section = 'another_new_section'
        option1 = 'another_new_option1'
        option2 = 'another_new_option2'
        value1 = 'replaced_value1'
        value2 = 'replaced_value2'        
        params = {}
        params[option1] = value1
        params[option2] = value2
        config.set_section(section, params)
        result = self._read_config(ConfigTests.filepath)
        assert section in result.keys(), "Error: section {} not found in {}.".format(section, result.keys()) 
        assert option1 in result[section].keys(), "Error: option {} not found in {}.".format(option1, result[section].keys()) 
        assert option2 in result[section].keys(), "Error: option {} not found in {section}.".format(option2, result[section].keys()) 
        assert value1 == result[section][option1], "Error: value {} not found in option {} of section {}".format(value1, option1, section)
        assert value2 == result[section][option2], "Error: value {} not found in option {} of section {}".format(value2, option2, section)

    def test_get_section(self):
        config = Config(ConfigTests.filepath)                
        section = 'another_new_section'
        option1 = 'another_new_option1'
        option2 = 'another_new_option2'
        value1 = 'replaced_value1'
        value2 = 'replaced_value2'        
        params = {}
        params[option1] = value1
        params[option2] = value2             
        result = config.get_section(section)
        assert result == params, "Error: Expected: {}\n     Actual: {}".format(params, result)

    def test_credentials(self):
        filepath = "./credentials.cfg"
        dbname ='bogus'

        cred = DBConfig(filepath)
        with pytest.raises(Exception):
            credentials = cred(dbname)
        dbname = 'metadata'
        credentials = cred(dbname)
        assert 'dbname' in credentials.keys(), "Error: getting credentials."
        assert 'port' in credentials.keys(), "Error: getting credentials."
        assert 'host' in credentials.keys(), "Error: getting credentials."
        assert 'password' in credentials.keys(), "Error: getting credentials."

    def test_datasources(self):
        filepath = './config.cfg'
        datasource = 'metadata'
        datasources = DataSourcesConfig(filepath)
        with pytest.raises(Exception):
            config = datasources(datasource)
        datasource = 'studies'
        config = datasources(datasource)
        sources = datasources.datasources
        print(sources)
        assert 'url' in config.keys(), "Error: getting datasource config."
        assert 'webpage' in config.keys(), "Error: getting datasource config."
        assert isinstance(sources, list), "Error: datasources method did not return a list "


def main():
    test = ConfigTests()
    test.test_set_config()
    test.test_get_config()
    test.test_set_section_new()
    test.test_set_section_existing()
    test.test_get_section()
    test.test_credentials()
    test.test_datasources()
    print("Configuration Tests Complete!")
    
if __name__ == '__main__':
    main()

#%%    