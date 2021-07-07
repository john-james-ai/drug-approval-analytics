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
# Modified : Wednesday, July 7th 2021, 12:56:14 am                             #
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
# -----------------------------------------------------------------------------#
class ORMDatabase(ABC):


    def __init__(self, credentials):        
        database_uri = "postgresql://{user}:{password}@{host}:{port}/{database}".format(
            user=credentials.user, password=credentials.password,
            host=credentials.host, port=credentials.port,
            database=credentials.database)

        self.engine = create_engine(database_uri)
        # Create all tables in the database that do not yet exist.
        Base.metadata.create_all(self.engine)
        # Create a session associated with the postgresql engine
        session = sessionmaker(self.engine)
        self.session = session()

    @abstractmethod
    def add(self, *args, **kwargs):
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

    @abstractmethod
    def drop(self):
        pass

    @abstractmethod   
    def close(self):
        pass
# -----------------------------------------------------------------------------#
class ORMDataSource(ORMDatabase):


    def __init__(self, credentials): 
        super(ORMDataSource, self).__init__(credentials)       

    def add(self, datasource):        
        self.session.add(datasource)
        self.session.commit()

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

    def drop(self):
        AACTDataSource.__table__.drop(self.engine)
        DrugsDataSource.__table__.drop(self.engine)
        LabelsDataSource.__table__.drop(self.engine)
        DataSource.__table__.drop(self.engine)
        
    def close(self):
        self.session.close()
# -----------------------------------------------------------------------------#
class ORMDataObject(ORMDatabase):


    def __init__(self, credentials): 
        super(ORMDataObject, self).__init__(credentials)       

    def add(self, dataobject):
        self.session.add(dataobject)
        self.session.commit()

    def read(self, name):
        return self.session.query(DataObject).filter(DataObject.name == name).all()

    def read_all(self):
        return self.session.query(DataObject).all()        

    def delete(self, name):
        self.session.query(DataObject).filter(DataObject.name == name).delete()
        self.session.commit()

    def delete_all(self):
        self.session.query(DataObject).delete()
        self.session.commit()
        
    def drop(self):
        DataObject.__table__.drop(self.engine)

    def close(self):
        self.session.close()