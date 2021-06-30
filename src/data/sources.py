#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#==============================================================================#
# Project  : Predict-FDA                                                       #
# Version  : 0.1.0                                                             #
# File     : \sources.py                                                       #
# Language : Python 3.9.5                                                      #
# -----------------------------------------------------------------------------#
# Author   : John James                                                        #
# Company  : nov8.ai                                                           #
# Email    : john.james@nov8.ai                                                #
# URL      : https://github.com/john-james-sf/predict-fda                      #
# -----------------------------------------------------------------------------#
# Created  : Tuesday, June 29th 2021, 9:45:46 pm                               #
# Modified : Wednesday, June 30th 2021, 1:09:04 am                             #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#
from abc import ABC, abstractmethod
from datetime import datetime

from configs.config import Config
# -----------------------------------------------------------------------------#
class DataSource:
    """Base class for data sources"""
    def __init__(self, name):
        self._name = name
        self.config = Config().get_config(section=name)        
        self._url = self.config['url']
        self._download_url = self.config['download_url']
        self._download_dir = self.config['download_dir']
        self._extract_dir = self.config['extract_dir']
        self._transform_dir = self.config['transform_dir']
    
    @property
    def name(self):
        return self._name

    @property
    def url(self):
        return self._url        

    @property
    def download_url(self):
        return self._download_url

    @property
    def download_dir(self):
        return self._download_dir

    @property
    def extract_dir(self):
        return self._extract_dir        

    @property
    def transform_dir(self):
        return self._transform_dir                

# -----------------------------------------------------------------------------#
class AACT(DataSource):
    """Defines the AACT postgres data source"""
    def __init__(self, name='aact'):
        super(AACT, self).__init__(name)        
        
    @property
    def filenames(self):
        files = []
        file =  datetime.now().strftime("%Y%m%d") + '_clinical_trials.zip'
        files.append(file)
        return files

# -----------------------------------------------------------------------------#
class DrugsFDA(DataSource):
    """Defines the Drugs@FDA Data source"""
    def __init__(self, name='drugsfda'):
        super(DrugsFDA, self).__init__(name)

# -----------------------------------------------------------------------------#
class OpenFDALabel(DataSource):
    """Defines OpenFDA Label data source"""
    def __init__(self, name='openfda_label'):
        super(OpenFDALabel, self).__init__(name)

    @property
    def filenames(self):
        return Config().get_config(self.name + '_files')

# -----------------------------------------------------------------------------#
class OpenFDA_NDC(DataSource):
    """Defines OpenFDA Label data source"""
    def __init__(self, name='openfda_ndc'):
        super(OpenFDA_NDC, self).__init__(name)

    @property
    def filenames(self):
        return Config().get_config(self.name + '_files')