#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#==============================================================================#
# Project  : Predict-FDA                                                       #
# Version  : 0.1.0                                                             #
# File     : \test_aact.py                                                     #
# Language : Python 3.9.5                                                      #
# -----------------------------------------------------------------------------#
# Author   : John James                                                        #
# Company  : nov8.ai                                                           #
# Email    : john.james@nov8.ai                                                #
# URL      : https://github.com/john-james-sf/predict-fda                      #
# -----------------------------------------------------------------------------#
# Created  : Saturday, June 26th 2021, 1:48:44 pm                              #
# Modified : Monday, June 28th 2021, 2:21:00 am                                #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#
import pytest, os

from configs.config import Config
from src.data.extract import ZipExtractor

@pytest.mark.extract
class ExtractorTests:

    def test_zip_extractor(self):        
        configerator = Config()
        config = configerator.get_config('openfda_label')
        destination = config['basedir']

        extractor = ZipExtractor(datasource='openfda_label')       
        if os.path.exists(destination):
            extractor.extract(force=True)
        else:
            extractor.extract()

        files  = os.listdir(destination)
        assert(len(files) == 10)





