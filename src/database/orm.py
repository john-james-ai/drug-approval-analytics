#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#==============================================================================#
# Project  : Predict-FDA                                                       #
# Version  : 0.1.0                                                             #
# File     : \orm.py                                                           #
# Language : Python 3.9.5                                                      #
# -----------------------------------------------------------------------------#
# Author   : John James                                                        #
# Company  : nov8.ai                                                           #
# Email    : john.james@nov8.ai                                                #
# URL      : https://github.com/john-james-sf/predict-fda                      #
# -----------------------------------------------------------------------------#
# Created  : Tuesday, July 6th 2021, 2:58:51 am                                #
# Modified : Sunday, July 11th 2021, 9:07:06 pm                                #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#
from abc import ABC, abstractmethod
from typing import List
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.database import Base
from src.data.datasources import DataSource, AACTDataSource, DrugsDataSource, LabelsDataSource
from src.data.dataobjects import DataObject
from configs.config import Credentials
# -----------------------------------------------------------------------------#
#                        ORM OBJECT FACTORY CLASSES                            #
# -----------------------------------------------------------------------------#
class ORMDatabaseFactory(ABC):

    def __init__(self, dbname='dara', empty=False):    
        self.dbname = dbname
        self.empty = empty    
        credentials = Credentials(dbname)
        ORMDatabase.initialize(credentials)
        if self.empty:
            ORMDatabase.rebuild()       

    @abstractmethod
    def create(self):
        pass    
    
# -----------------------------------------------------------------------------#
class DataSourcesFactory(ORMDatabaseFactory):
    """Creates a session with the DataSources table."""

    def __init__(self, dbname='dara', empty=False):
        super(DataSourcesFactory, self).__init__(dbname, empty)

    def create(self):
        session =  ORMDatabase.get_session()            
        return ORMDataSource(session)


# -----------------------------------------------------------------------------#
class DataObjectsFactory(ORMDatabaseFactory):
    """Creates a session with the DataObjects table."""

    def __init__(self, dbname='dara', empty=False):
        super(DataObjectsFactory, self).__init__(dbname, empty)

    def create(self):
        session =  ORMDatabase.get_session()            
        return ORMDataObject(session)


# -----------------------------------------------------------------------------#
#                             ORM DATABASE                                     #        
# -----------------------------------------------------------------------------#
class ORMDatabase:

    __engine = None
    __session = None

    @staticmethod
    def initialize(credentials):
        database_uri = "postgresql://{user}:{password}@{host}:{port}/{database}".format(
            user=credentials.user, password=credentials.password,
            host=credentials.host, port=credentials.port,
            database=credentials.database)

        ORMDatabase.__engine = create_engine(database_uri)    
        # Create all tables in the database that do not yet exist.
        Base.metadata.create_all(ORMDatabase.__engine)
        # Create a session associated with the postgresql engine
        session = sessionmaker(ORMDatabase.__engine)        
        ORMDatabase.__session = session()

    @staticmethod
    def get_engine():
        return ORMDatabase.__engine

    @staticmethod
    def get_session():
        return ORMDatabase.__session

    @staticmethod
    def rebuild():
        Base.metadata.drop_all(ORMDatabase.__engine)
        Base.metadata.create_all(ORMDatabase.__engine)       

    @staticmethod
    def drop():
        Base.metadata.drop_all(ORMDatabase.__engine)

    @staticmethod
    def close():
        ORMDatabase.__session.close()

# -----------------------------------------------------------------------------#
#                             ORM TABLE CLASSES                                #        
# -----------------------------------------------------------------------------#
class ORMTable(ABC):

    def __init__(self, session):        
        self.session = session

    def __iter__(self):
        items = self.read_all()
        for item in items:
            yield item.name        

    def list(self):
        items = self.read_all()
        return [item.name for item in items]
    
    @abstractmethod
    def add(self, *args, **kwargs):    
        pass

    @abstractmethod
    def update(self, *args, **kwargs):
        pass

    @abstractmethod
    def read(self, *args, **kwargs):
        pass

    @abstractmethod
    def read_all(self):
        pass

    @abstractmethod
    def delete(self, *args, **kwargs):
        pass

    @abstractmethod
    def delete_all(self):
        pass
# -----------------------------------------------------------------------------#
class ORMDataSource(ORMTable):    


    def add(self, datasource):        
        self.session.add(datasource)
        self.session.commit()

    def update(self, datasource, column):
        value = getattr(datasource, column)
        self.session.query(DataSource).filter(DataSource.name==datasource.name).\
            update({column: value}, synchronize_session='fetch')
        self.session.commit()        

    def exists(self, name):          
        if not self.engine.has_table('datasource'):
            return False
        else:      
            query = self.session.query(DataSource).filter(DataSource.name == name)
            return self.session.query(query.exists()).scalar()

    def read(self, name):
        return self.session.query(DataSource).filter(DataSource.name == name).all()        

    def read_all(self):        
        return self.session.query(DataSource).all()                

    def delete(self, name):
        self.session.query(DataSource).filter(DataSource.name == name).delete()
        self.session.commit()

    def delete_all(self):
        self.session.query(LabelsDataSource).delete()
        self.session.query(DrugsDataSource).delete()
        self.session.query(AACTDataSource).delete()
        self.session.query(DataSource).delete()
        self.session.commit()

    def __eq__(self, other):
        return self.name == other.name &\
                self.webpage == other.webpage &\
                self.baseurl == other.baseurl &\
                self.url == other.url &\
                self.lifecycle == other.lifecycle & \
                self.last_extracted.date() == other.last_extracted.date()
                


# -----------------------------------------------------------------------------#
class ORMDataObject(ORMTable):


    def add(self, dataobject):        
        self.session.add(dataobject)
        self.session.commit()

    def update(self, dataobject, column):
        value = getattr(dataobject, column)
        self.session.query(DataObject).filter(DataObject.name==dataobject.name).\
            update({column: value}, synchronize_session='fetch')
        self.session.commit()        

    def exists(self, name):  
        if not self.engine.has_table('dataobject'):
            return False
        else:      
            query = self.session.query(DataObject).filter(DataObject.name == name)
            return self.session.query(query.exists()).scalar()

    def read(self, name):
        return self.session.query(DataObject).filter(DataObject.name == name).all()        

    def read_all(self):        
        return self.session.query(DataObject).all()                

    def delete(self, name):
        self.session.query(DataObject).filter(DataObject.name == name).delete()
        self.session.commit()

    def delete_all(self):
        self.session.query(FileDataObject).delete()
        self.session.query(DirectoryDataObject).delete()        
        self.session.query(DataObject).delete()
        self.session.commit()

    def __eq__(self, other):
        return self.name == other.name &\
            self.description == other.description &\
                self.persistence == other.persistence &\
                    self.originator == other.originator


    
