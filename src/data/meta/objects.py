#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#==============================================================================#
# Project  : Drug Approval Analytics                                           #
# Version  : 0.1.0                                                             #
# File     : \src\data\metadata\objects.py                                     #
# Language : Python 3.9.5                                                      #
# -----------------------------------------------------------------------------#
# Author   : John James                                                        #
# Company  : nov8.ai                                                           #
# Email    : john.james@nov8.ai                                                #
# URL      : https://github.com/john-james-sf/drug-approval-analytics          #
# -----------------------------------------------------------------------------#
# Created  : Sunday, July 18th 2021, 12:06:03 am                               #
# Modified : Sunday, July 18th 2021, 4:37:29 am                                #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#
"""Class defines the metadata for data sources, entities and attributes.

This module first defines the metadata object, i.e. the DataSource, then its
data access object.
"""
from datetime import datetime

from sqlalchemy import Column, String, Integer, DateTime, ForeignKey, Identity
from sqlalchemy.orm import relationship

from ...utils.config import DataSourcesConfig
from ..database.orm import Base
DBNAME = 'metadata'
# ---------------------------------------------------------------------------- #
#                               DATASOURCE                                     #
# ---------------------------------------------------------------------------- #
class DataSource(Base):
    """Class defining a data source used in this project.

    Arguments
    ----------
        name (str): The name of the data source

    Attributes
    ----------        
        name (str): Name of object. 
        title (str): The title for the source.
        description (str): A text description for reporting purposes        
        creator (str): The organization responsible for producing the source.
        maintainer (str): The organization that maintains the data and its distribution        
        webpage (str): Webpage containing the data
        url_type (str): The type of url, i.e. baseurl or direct to the resource
        url (str): The url value of the aforementioned type.
        media_type (str): The format, or more formally, the MIME type of the data as per RFC2046        
        frequency (str): The frequency by which the source is updated.  
        coverage (str): The temporal range of the data if available.
        lifecycle (int): The number of days between data refresh at the source             
        last_updated (DateTime): The date the source links were last updated
        last_extracted (DateTime): The date the source was last extracted
        last_staged (DateTime): The date the source was last staged

        created (DateTime): The datetime this object was created
        updated (DateTime): The datetime this object was last updated.
    """    
    __tablename__ = 'datasources'

    id = Column(Integer, Identity(), primary_key=True,)
    name = Column('name', String(20), primary_key=True, unique=True)
    title = Column('title', String(120))
    description = Column('description', String(512))
    creator = Column('creator', String(120))
    maintainer = Column('maintainer', String(120))
    webpage = Column('webpage', String(80))
    url_type = Column('url_type', String(80))
    url = Column('url', String(120))
    media_type =  Column('media_type', String(16))
    frequency =  Column('frequency', String(32))
    coverage =  Column('coverage', String(80))
    lifecycle = Column('lifecycle', Integer)
    last_updated = Column('last_updated', DateTime)
    last_extracted = Column('last_extracted', DateTime)
    last_staged = Column('last_staged', DateTime)
    created = Column('created', DateTime)
    updated = Column('updated', DateTime)    

    entities = relationship("Entity", back_populates="datasource")
    
    def __init__(self, name):
        config = DataSourcesConfig()
        config(name)        

        self.name = name
        self.title = config.title
        self.description = config.description
        self.creator = config.creator
        self.maintainer = config.maintainer
        self.webpage = config.webpage
        self.url = config.url
        self.url_type = config.url_type
        self.media_type = config.media_type
        self.frequency = config.frequency
        self.coverage = config.coverage
        self.lifecycle = config.lifecycle
        self.last_updated = config.last_updated
        self.last_extracted = config.last_extracted
        self.last_staged = config.last_staged

        self.created = datetime.now()
        self.updated = datetime.now()


    def __str__(self):
        text = ""
        for key, value in self.__dict__.items():
            if not key.startswith("_sa"):
                text += "    {} = {}".format(key,value)
                text += '\n'
        return (
            "\n" +
            self.__class__.__name__ +
            "\n---------\n" +
            text + 
            "---------")        

    def __repr__(self):
        return "{classname}({name})".format(
            classname=self.__class__.__name__,
            name=repr(self.name))
            
    def __eq__(self, other):
        equal = True
        for k, v in self.__dict__.items():
            if not k.startswith('_'):
                if equal:
                    equal = other.__dict__[k] == v
        return equal


# ---------------------------------------------------------------------------- #
#                                   ENTITY                                     #
# ---------------------------------------------------------------------------- #

class Entity(Base):
    """Class defining a data entity used in this project.

    Arguments
    ----------
        name (str): The name of the data source
        source_name (str): Foreign key to the DataSource object from which it derives.

    Attributes
    ----------        
        title (str): The capital case title for the entity
        description (str): The text that describes the entity        
        
        created (DateTime): The datetime this object was created
        updated (DateTime): The datetime this object was last updated.
    """    
    __tablename__ = 'entities'

    id = Column(Integer, primary_key=True)
    name = Column('name', String(20), primary_key=True, unique=True)
    datasource_name = Column('datasource_name', String(20), ForeignKey('datasources.name'))
    title = Column('title', String(120))
    description = Column('description', String(240))
    created = Column('created', DateTime)
    updated = Column('updated', DateTime)

    datasource = relationship("DataSource", back_populates='entities')
    attributes = relationship("Attribute",  back_populates='entity')
    
    def __init__(self, name, source_name):
        self.name = name
        self.datasource_name = datasource_name         
        self.title = None
        self.description = None
        self.created = datetime.now()
        self.updated = datetime.now()

    def __str__(self):
        text = ""
        for key, value in self.__dict__.items():
            if not key.startswith("_sa"):
                text += "    {} = {}".format(key,value)
                text += '\n'
        return (
            "\n" +
            self.__class__.__name__ +
            "\n---------\n" +
            text + 
            "---------")     

    def __repr__(self):
        return "{classname}({name})".format(
            classname=self.__class__.__name__,
            name=repr(self.name),
            source_name=repr(self.source_name))


# ---------------------------------------------------------------------------- #
#                                ATTRIBUTE                                     #
# ---------------------------------------------------------------------------- #
class Attribute(Base):
    """Class defining an attribute associated with an entity.

    Arguments
    ----------
        name (str): The name of the data source
        entity_name (str): The name of the entity to which this attribute belongs.        
        datatype (str): Python datatype  

    Attributes
    ----------                
        description (str): The meaning of the attribute.
        dataclass (str): Nominal,Ordinal, Discrete, Continuous, Interval, Ratio    
        category (str): A classification or grouping for the attribute.    
        created (DateTime): The datetime this object was created
        updated (DateTime): The datetime this object was last updated.
    """    
    __tablename__ = 'attributes'    

    id = Column(Integer, primary_key=True)
    name = Column('name', String(20), primary_key=True, unique=True)
    entity_name = Column('entity_name', String(20), ForeignKey('entities.name'))    
    description = Column('description', String(240))
    datatype = Column('datatype', String(20))    
    dataclass = Column('dataclass', String(20))    
    category = Column('category', String(40))    
    created = Column('created', DateTime)
    updated = Column('updated', DateTime)

    entity = relationship("Entity", back_populates='attributes')

    def __init__(self, name, entity_name, datatype):
        self.name = name
        self.entity_name = entity_name
        self.datatype = datatype

    def __str__(self):
        text = ""
        for key, value in self.__dict__.items():
                text += "{} = {}".format(key,value)
                text += '\n'
        return (
            self.__class__.__name__ +
            "\n----------" +
            text + 
            "---------")                

    def __repr__(self):
        return "{classname}({name})".format(
            classname=self.__class__.__name__,
            name=repr(self.name),
            entity_name=repr(self.entity_name),
            datatype=repr(self.datatype))        
  
