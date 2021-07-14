#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#==============================================================================#
# Project  : Predict-FDA                                                       #
# Version  : 0.1.0                                                             #
# File     : \metadata.py                                                      #
# Language : Python 3.9.5                                                      #
# -----------------------------------------------------------------------------#
# Author   : John James                                                        #
# Company  : nov8.ai                                                           #
# Email    : john.james@nov8.ai                                                #
# URL      : https://github.com/john-james-sf/predict-fda                      #
# -----------------------------------------------------------------------------#
# Created  : Thursday, July 1st 2021, 7:38:20 pm                               #
# Modified : Friday, July 2nd 2021, 12:48:26 am                                #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#
#%%
import os, json
from datetime import datetime

from config.config import Config
# -----------------------------------------------------------------------------#
class Metadata:
    """Creates and manages metadata for the database and pipeline."""
    def __init__(self, config):
        self._config = config
        self._metadata_dir = self._config.get('directories')['metadata']

    def save(self, source, name, metadata):
        """Saves the metadata in dictionary format for each dataobject."""        
        metadata_file = os.path.join(self._metadata_dir, source, name + '.json')
        json_string = json.dumps(metadata)
        json_file = open(metadata_file, "w")
        json_file.write(json_string)
        json_file.close()
        
    def read(self, source, name):
        """Reads metadata from file."""
        metadata_file = os.path.join(self._metadata_dir, source, name + '.json')
        json_file = open(metadata_file, "r")
        json_string = json_file.read()
        return json.loads(json_string)


