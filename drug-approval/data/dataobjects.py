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
# Modified : Sunday, July 11th 2021, 8:42:43 pm                                #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#
import inspect
import os
from datetime import date, datetime
import pprint
import json

import pandas as pd
from sqlalchemy import String, Integer, Column, Boolean, DateTime
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.schema import ForeignKey
from approval.database import Base
from approval.utils.files import get_file_metadata
# -----------------------------------------------------------------------------#
class DataObject(Base):    
    """Base class for storage independent data that flows through the pipeline.

    A DataObject is groups related data items, such as clinical studies, 
    combined into a single unit. DataObjects are analogous to 2D data tables
    containing rows and columns.

    Parameters
    ----------
        name (str): Name for the DataObject. This must be unique in the pipeline.
        entity (str): Name representing the meaning of the object.
        data (DataFrame): DataFrame containing the data.

    Attributes:       
    -----------                  
        created (DateTime): The datatime the object was created.
        updated (DateTime): The datatime the object was updated.
         
    """

    __tablename__ = 'dataobject'

    id = Column(Integer, primary_key=True)
    _name = Column('name', String(20)) 
    _entity = Column('entity', String(20)) 
    _created = Column('created', DateTime)
    _updated = Column('updated', DateTime)

    # Supports Polymorphism
    type = Column(String)  

    __mapper_args__ = {
        'polymorphic_identity': 'dataobject',
        'polymorphic_on': type
    }    
    
    def __init__(self, name, entity, data):        
        self._name = name         
        self._entity = entity
        self._data = data
        self._created = datetime.now()
        self._updated = datetime.now()        

        self._data = None

    def __repr__(self):        
        return  f'self.__class__.__name__('\
                f'{repr(self._name)}, {repr(self._entity)})'

    def __str__(self):
        classname = "{classname}\n".format(classname=self.__class__.__name__)
        attributes = json.dumps(self.__dict__, indent=2, separators=(',',':'))
        attributes, readable, recursion = pprint.format(attributes, depth=1)
        return classname + attributes

    # ----------------------------------------------------------------------- #
    #                           PUBLIC METHODS                                #
    # ----------------------------------------------------------------------- #
    def get_data(self):
        return self._data

    def set_data(self, df):
        self._data = df

    def get_attributes(self):        
        return self._data.dtypes.to_frame()               

    # ----------------------------------------------------------------------- #
    #                              PROPERTIES                                 #
    # ----------------------------------------------------------------------- #
    @hybrid_property
    def name(self):
        return self._name

    @hybrid_property
    def created(self):
        return self._created

    @hybrid_property
    def updated(self):
        return self._updated

