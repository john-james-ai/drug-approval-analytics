#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#==============================================================================#
# Project  : Predict-FDA                                                       #
# Version  : 0.1.0                                                             #
# File     : \database.py                                                      #
# Language : Python 3.9.5                                                      #
# -----------------------------------------------------------------------------#
# Author   : John James                                                        #
# Company  : nov8.ai                                                           #
# Email    : john.james@nov8.ai                                                #
# URL      : https://github.com/john-james-sf/predict-fda                      #
# -----------------------------------------------------------------------------#
# Created  : Thursday, July 15th 2021, 11:58:44 am                             #
# Modified : Sunday, July 18th 2021, 1:37:53 am                                #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#
import os
import shutil
from datetime import datetime
from sys import exit
import logging
logger = logging.getLogger(__name__)

from sqlalchemy import MetaData
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from ...utils.config import Credentials
# -----------------------------------------------------------------------------#
Base = declarative_base()
# -----------------------------------------------------------------------------#
#                       SQLALCHEMY ORM DATABASE FACTORY                        #
# -----------------------------------------------------------------------------#
class ORMDatabaseFactory:
    """ORM database factory"""

    def __init__(self, dbname):
        self._dbname = dbname
        self._engine = self._create_engine()
        Base.metadata.create_all(self._engine)

    def _create_engine(self):

        # Postgres dbname  
        master_dbname = 'postgres'

        # Credentials are stored in credentials.cfg file by dbname
        credentials = Credentials()
        credentials(self._dbname)
        
        # URI for the database to be created in postgresql format
        uri = f'postgresql://{credentials.user}:{credentials.password}' \
                       f'@{credentials.host}:{credentials.port}/'        

        # SQLAlchemy database engine representing behaviors for the postgres database
        return create_engine(f'{uri}{master_dbname}')            


    @property
    def engine(self):
        return self._engine

    @property
    def session(self):
        session = sessionmaker(self._engine)
        return session()

metadatadb = ORMDatabaseFactory('metadata')
pipelinedb = ORMDatabaseFactory('pipeline')