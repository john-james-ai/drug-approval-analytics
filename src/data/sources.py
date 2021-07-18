#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#==============================================================================#
# Project  : Drug Approval Analytics                                           #
# Version  : 0.1.0                                                             #
# File     : \src\data\sources.py                                              #
# Language : Python 3.9.5                                                      #
# -----------------------------------------------------------------------------#
# Author   : John James                                                        #
# Company  : nov8.ai                                                           #
# Email    : john.james@nov8.ai                                                #
# URL      : https://github.com/john-james-sf/drug-approval-analytics          #
# -----------------------------------------------------------------------------#
# Created  : Sunday, July 18th 2021, 6:01:17 am                                #
# Modified : Sunday, July 18th 2021, 8:01:38 am                                #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#
"""Class defines the data sources used in this project.

Add data sources descend from a DataSource base class that defines the 
API for the DataSource sub-classes. Each DataSource object encapsulates the
data and behaviors required to extract both data and any available metadata
provided by the source. 

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
        link (str): Direct or base link to the data resource
        is_direct (boolean): Indicates whether the link is direct or a baselink
        url (str): The url to the data source.
        has_metadata (bool): Indicates whether metadata urls are available
        entity_metadata_url (str): The url to the entity or table metadata
        attribute_metadata_url (str): The url to the attribute metadata.
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
    link = Column('link', String(80))    
    is_direct = Column('is_direct', Boolean)
    urls = Column('urls', ARRAY(String))
    has_metadata = Column('has_metadata', Boolean)
    entity_metadata_url = Column('entity_metadata_link', String(120))
    attribute_metadata_url = Column('attribute_metadata_link', String(120))        
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
        self.link = config.link
        self.is_direct = config.is_direct   
        self.has_metadata = config.has_metadata  
        self.entity_metadata_url = config.entity_metadata_url   
        self.attribute_metadata_url = config.attribute_metadata_url   
        self.media_type = config.media_type
        self.frequency = config.frequency
        self.coverage = config.coverage
        self.lifecycle = config.lifecycle
        self.last_updated = config.last_updated
        self.last_extracted = config.last_extracted
        self.last_staged = config.last_staged

        self.urls = None

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

    def update(self):
        """Updates the url and its date last updated"""
        pass

    def is_expired(self):
        return  datetime.now() > (self.last_extracted + \
            timedelta(days=self.lifecycle))             

    def has_updated(self):
        return self.last_updated > self.last_extracted
    
# ---------------------------------------------------------------------------- #
#                               AACTDataSource                                 #
# ---------------------------------------------------------------------------- #
class AACTDataSource(DataSource):
    """Aggregate Analysis of ClinicalTrials.gov database"""

    __tablename__ = 'aact_datasource'

    id = Column(Integer, ForeignKey('datasource.id'), primary_key=True)  

    __mapper_args__ = {
        'polymorphic_identity': 'aact_datasource',
    } 

    def update(self):
        """Updates the url and its date last updated."""

        response = requests.get(self.webpage)   
        # Use BeautifulSoup to create searchable html parser
        soup = BeautifulSoup(response.content, 'html.parser')    

        # GET URL
        # There are two main tables on the webpage: one for daily static 
        # copies and the other for monthly archives. The target url is 
        # in the first table under the heading 'Current Month's Daily Static Copies'
        table = soup.findChildren('table')[0]    
        # Grab the first row, which is the latest archive.
        row = table.find_all('tr')[0]                
        # Get the link from the href element
        link = row.find('a', href=True)['href']
        # Confirm that link is from the daily static table.
        self.urls = []
        if 'daily' in link:            
            url = self.baseurl + link
            self.urls.append(url) 

        # GET DATE LAST UPDATE
        # If the link is valid, format the full url and obtain its timestamp
        # and update the member variables.
        if self.urls:
            # Get the date the url was created from the same row.
            date_string = row.find_all('td')[1].text
            # Convert the text to datetime object
            self.last_updated = datetime.strptime(date_string, "%m/%d/%Y")

        self.updated = datetime.now()    

# ---------------------------------------------------------------------------- #
#                               DRUGS DATASOURCE                               #
# ---------------------------------------------------------------------------- #
class DrugsDataSource(DataSource):
    """Obtains data from Drugs@FDA site"""     
    
    __tablename__ = 'drugs_datasource'

    id = Column(Integer, ForeignKey('datasource.id'), primary_key=True)   

    __mapper_args__ = {
        'polymorphic_identity': 'drugs_datasource',
    }     

    def update(self):
        """Obtains and validates urls and timestamps associated with the data"""        
        response = requests.get(self.webpage)   
        response.raise_for_status()
        # Use BeautifulSoup to create searchable html parser
        soup = BeautifulSoup(response.content, 'html.parser')            
        link = soup.find(attrs={"data-entity-substitution":"media_download"})["href"]
        # Format url and add to list object.
        url = self.baseurl + link
        self.urls = []
        self.urls.append(url)

        # Find and parse the timestamp for the link.
        text = soup.find_all(string=re.compile("Data Last Updated:"))[0]
        self.last_updated = Parser().drugs_last_updated(text)           

        # Update the timestamp on this object
        self.updated = datetime.now()              
# ---------------------------------------------------------------------------- #
#                           LABELS DATASOURCE                                  #
# ---------------------------------------------------------------------------- #
class ChEMBLDataSource(DataSource):
    """ChEMBL Postgres Database."""
    
    __tablename__ = 'chembl_datasource'

    id = Column(Integer, ForeignKey('datasource.id'), primary_key=True)       

    __mapper_args__ = {
        'polymorphic_identity': 'chembl_datasource',
    }         

       
    def update(self):
        """Obtains and validates urls and timestamps associated with the data"""        
        response = requests.get(self.webpage)   
        response.raise_for_status()
        # Use BeautifulSoup to create searchable html parser
        soup = BeautifulSoup(response.content, 'html.parser')            
        link = soup.find_all('a', href=re.compile("https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/latest/chembl_28_postgresql.tar.gz"))
        # Format url and add to list object.
        url = self._baseurl + link
        self.urls = []
        self.urls.append(url)

        # Find and parse the timestamp for the link.
        text = soup.find_all(string=re.compile("Data Last Updated:"))[0]
        self.last_updated = Parser().drugs_last_updated(text)           

        # Update the timestamp on this object
        self.updated = datetime.now()   

# ---------------------------------------------------------------------------- #
#                           LABELS DATASOURCE                                  #
# ---------------------------------------------------------------------------- #
class LabelsDataSource(DataSource):
    """Drug label information from Labels."""
    
    __tablename__ = 'labels_datasource'

    id = Column(Integer, ForeignKey('datasource.id'), primary_key=True)       

    __mapper_args__ = {
        'polymorphic_identity': 'labels_datasource',
    }         

        
    def update(self):
        """Obtains and validates urls and timestamps associated with the data"""        

        # First, obtain the json containing all download links for the site.        
        response = urlopen(self.link)
        links = json.loads(response.read())     

        # Extract all the relevant links. The data are split into 10
        # partitions, each with its own link.      
        partitions = links['results']['drug']['label']['partitions']
        self.urls = []
        for partition in partitions:
            self.urls.append(partition['file'])
        
        # Extract the timestamp for those links.
        last_updated_text = links['results']['drug']['label']['export_date']
        self.last_updated = datetime.strptime(last_updated_text, "%Y-%m-%d")
        
        self._updated = datetime.now()
