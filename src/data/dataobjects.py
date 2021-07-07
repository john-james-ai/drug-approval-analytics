#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#==============================================================================#
# Project  : Predict-FDA                                                       #
# Version  : 0.1.0                                                             #
# File     : \dataobject.py                                                    #
# Language : Python 3.9.5                                                      #
# -----------------------------------------------------------------------------#
# Author   : John James                                                        #
# Company  : nov8.ai                                                           #
# Email    : john.james@nov8.ai                                                #
# URL      : https://github.com/john-james-sf/predict-fda                      #
# -----------------------------------------------------------------------------#
# Created  : Thursday, July 1st 2021, 11:55:27 am                              #
# Modified : Tuesday, July 6th 2021, 7:47:54 pm                                #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#
import inspect
import os
from datetime import date, datetime

from sqlalchemy import String, Integer, Column, Boolean, DateTime
from sqlalchemy.orm import declarative_base
Base = declarative_base()
# -----------------------------------------------------------------------------#
class DataObject(Base):    
    """Standard object that represents and provides access to data.

    Attributes:        
        name (str): name of object. 
        source (str): Name of the source data item
        otype (str): Type of DataObject, i.e. 'clinical_trial', 'drug', etc...
        stage (str): Pipeline stage, i.e. 'extracted', 'staged', etc...
        originator (str): Name of the class and method that instantiated this object.
        persistence (str): Filepath where data is stored                
         
    """

    __abstract__ = True

    id = Column(Integer, primary_key=True)
    name = Column('name', String)
    source = Column('source', String)    
    otype = Column('otype', String)
    stage = Column('stage', String)
    persistence = Column('persistence', String)
    originator = Column('originator', String)
    
    def __init__(self, name, source, otype, stage, persistence, *args, **kwargs):        
        self.name = name 
        self.source = source
        self.otype = otype
        self.stage = stage
        self.persistence = persistence        
        self.profile = None
        self.metadata = None
        stack = inspect.stack()
        self.originator = stack[1][0].f_locals["self"].__class__.__name__

    def __repr__(self):
        return "DataObject({},{},{},{},{})".format(repr(self.name), repr(self.source), repr(self.otype),
                                repr(self.stage), repr(self.persistence))

    def __str__(self):
        return "DataObject({},{},{},{},{})".format(self.name, self.source, self.otype,
                                self.stage, self.persistence)

    def get_data(self):
        pass

    def set_data(self, data):
        pass
