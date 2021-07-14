#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#==============================================================================#
# Project  : Predict-FDA                                                       #
# Version  : 0.1.0                                                             #
# File     : \steps.py                                                         #
# Language : Python 3.9.5                                                      #
# -----------------------------------------------------------------------------#
# Author   : John James                                                        #
# Company  : nov8.ai                                                           #
# Email    : john.james@nov8.ai                                                #
# URL      : https://github.com/john-james-sf/predict-fda                      #
# -----------------------------------------------------------------------------#
# Created  : Friday, July 9th 2021, 10:14:03 pm                                #
# Modified : Monday, July 12th 2021, 3:54:25 am                                #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#
import os
import requests
from zipfile import ZipFile
from io import BytesIO
from datetime import datetime
import json

from bs4 import BeautifulSoup
import pandas as pd

from approval.pipeline.core import PipelineStep
from approval.logging import log_step
# -----------------------------------------------------------------------------#
class Extract(PipelineStep):
    """Extracts the data from the sources
    
    Arguments
    ---------
        name (str): The name of the PipelineStep object        
        seq (int): The sequence in which this task is executed in the stage    

    """    
    stage = "Extract"

    def __init__(self, name, seq):
        super(Extract, self).__init__(name, seq)
    
    def _download(self, response, path):      
        """Downloads the resource."""
        zipfile = ZipFile(BytesIO(response.content))
        zipfile.extractall(path)            

    @log_step
    def execute(self, dataobject):
        # Refresh the links and timestamps for the data source.
        dataobject.update_data_source_information()
        if dataobject.is_refreshable:
            response = requests.get(dataobject.url)
            response.raise_for_status()
            self._download(response, dataobject.extract_dir)
            dataobject.last_extracted = datetime.now()
        dataobject.last_executed = datetime.now()
        return dataobject
        
# -----------------------------------------------------------------------------#
class Stage(PipelineStep):
    """Interface and context class (I know, I know) for Stage objects.  

    Arguments
    ---------
        name (str): The name of the PipelineStep object        
        seq (int): The sequence in which this task is executed in the stage    

    """    
    stage = "Extract"

    def __init__(self, name, seq):
        super(Stage, self).__init__(name, seq)
        self._strategy = None

    def set_strategy(self, strategy):
        self._strategy = strategy

    @log_step
    def execute(self, dataobject):
        if dataobject.last_staged < dataobject.last_extracted:
            return self._strategy.execute(dataobject)
        else:
            return dataobject

# -----------------------------------------------------------------------------#
class StagePGDatabase(PipelineStep):
    """Extracts data from the PostgreSQL Database and stages it in csv format.

    Arguments
    ---------
        name (str): The name of the PipelineStep object        
        seq (int): The sequence in which this task is executed in the stage    

    """   
    stage = "Extract"

    def __init__(self, name, seq, database_access_object):
        super(StagePGDatabase, self).__init__(name, seq)
        self._database_access_object = database_access_object
    
    def execute(self, dataobject):
        """Commands the staging process."""
        tables = self._database_access_object.tables
        
        for table in tables:            
            df = self._database_access_object.read_table(table)
            filename = table + ".csv"
            filepath = os.path.join(dataobject.staging_dir, filename)
            df.to_csv(filepath)
        dataobject.last_staged = datetime.now()
        return dataobject

# -----------------------------------------------------------------------------#
class StageTSVFiles(PipelineStep):
    """Extracts data from TSV files and stages it in csv format.

    Arguments
    ---------
        name (str): The name of the PipelineStep object        
        seq (int): The sequence in which this task is executed in the stage    

    """   
    stage = "Extract"

    def __init__(self, name, seq):
        super(StageTSVFiles, self).__init__(name, seq)
    
    def execute(self, dataobject):
        """Commands the staging process."""
        location = dataobject.extract_dir
        filenames = os.listdir(location)
        for filename in filenames:
            filepath = os.path.join(dataobject.extract_dir, filename)
            df = pd.read_csv(filepath, sep='\t', error_bad_lines=False)
            filename = filename.replace('.txt', '.csv')
            filepath = os.path.join(dataobject.staging_dir, filename)
            df.to_csv(filepath)
        dataobject.last_staged = datetime.now()
        return dataobject

# -----------------------------------------------------------------------------#
class StageJSONFiles(PipelineStep):
    """Extracts data from JSON files and stages it in csv format.

    Arguments
    ---------
        name (str): The name of the PipelineStep object        
        seq (int): The sequence in which this task is executed in the stage    

    """   
    stage = "Extract"

    def __init__(self, name, seq):
        super(StageJSONFiles, self).__init__(name, seq)
    
    def execute(self, dataobject):
        """Commands the staging process."""
        location = dataobject.extract_dir
        filenames = os.listdir(location)
        for filename in filenames:
            filepath = os.path.join(dataobject.extract_dir, filename)
            data = json.load(open(filepath))
            df = pd.DataFrame(data['results'])
            filename = filename.replace('.json', '.csv')
            filepath = os.path.join(dataobject.staging_dir, filename)
            df.to_csv(filepath)
        dataobject.last_staged = datetime.now()
        return dataobject