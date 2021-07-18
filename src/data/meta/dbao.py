#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#==============================================================================#
# Project  : Drug Approval Analytics                                           #
# Version  : 0.1.0                                                             #
# File     : \src\data\meta\db.py                                              #
# Language : Python 3.9.5                                                      #
# -----------------------------------------------------------------------------#
# Author   : John James                                                        #
# Company  : nov8.ai                                                           #
# Email    : john.james@nov8.ai                                                #
# URL      : https://github.com/john-james-sf/drug-approval-analytics          #
# -----------------------------------------------------------------------------#
# Created  : Sunday, July 18th 2021, 12:09:04 am                               #
# Modified : Sunday, July 18th 2021, 4:34:48 am                                #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#

from ..database.orm import Base, metadatadb
from .objects import DataSource, Entity, Attribute
from ...utils.config import DataSourcesConfig
# -----------------------------------------------------------------------------#
class DataSources:

    def __init__(self):
        self._database = metadatadb
        self._engine = metadatadb.engine
        Base.metadata.drop_all(self._engine)
        Base.metadata.create_all(self._engine)
        self._session = metadatadb.session        
        self._config = DataSourcesConfig()

    def __equal__(self, a,b):
        a_attrs = a.__dict__
        del a_attrs['id']
        del a_attrs['created']
        del a_attrs['updated']
        b_attrs = b.__dict__
        del b_attrs['id']
        del b_attrs['created']
        del b_attrs['updated']        
        return a_attrs == b_attrs

    def load(self):
        datasources = self._config.datasources
        for source in datasources:
            datasource = DataSource(source)
            self._session.add(datasource)
            self._session.commit()
        self._session.close()

    def get(self, name=None):
        if name is None:
            datasources = self._session.query(DataSource).all()
        else:
            datasources = self._session.query(DataSource).filter(DataSource.name == name).one()
        return datasources

    def add(self, datasource):
        self._session.add(datasource)
        self._session.commit()

    def update_last_updated(self, name, last_updated):
        self._session.query(DataSource).filter(DataSource.name == name).\
            update({"last_updated": last_updated}, synchronize_session="fetch")
        self._session.commit()

    def update_last_extracted(self, name, last_extracted):
        self._session.query(DataSource).filter(DataSource.name == name).\
            update({"last_extracted": last_extracted}, synchronize_session="fetch")
        self._session.commit()       

    def update_last_staged(self, name, last_staged):
        self._session.query(DataSource).filter(DataSource.name == name).\
            update({"last_staged": last_staged}, synchronize_session="fetch")
        self._session.commit()       

    def copy(self, datasource):        
        name = datasource.name 
        new_datasource = DataSource(name)       
        for k,v in datasource.__dict__.items():
            if not k.startswith('_'):
                new_datasource.__dict__[k] = v
        return new_datasource
       

    def delete(self, name=None):
        if name is None:
            self._session.query(DataSource).delete()
        else:
            self._session.query(DataSource).filter(DataSource.name == name).delete()
        self._session.commit()


    def list(self):
        return [source.name for source in self.get()]

