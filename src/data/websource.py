#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#==============================================================================#
# Project  : Predict-FDA                                                       #
# Version  : 0.1.0                                                             #
# File     : \websource.py                                                     #
# Language : Python 3.9.5                                                      #
# -----------------------------------------------------------------------------#
# Author   : John James                                                        #
# Company  : nov8.ai                                                           #
# Email    : john.james@nov8.ai                                                #
# URL      : https://github.com/john-james-sf/predict-fda                      #
# -----------------------------------------------------------------------------#
# Created  : Sunday, June 27th 2021, 1:56:10 am                                #
# Modified : Sunday, June 27th 2021, 5:05:29 pm                                #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#
#%%
import requests, os
from urllib.request import urlopen
from io import BytesIO
from zipfile import ZipFile
from bs4 import BeautifulSoup

from configs.config import get_config

class Scraper:
    '''Scrapes data from a website'''

    def __init__(self, config_section):         
        self.config = get_config(config_section)
        self.baseurl = self.config["baseurl"]
        self.downloadurl = self.config["downloadurl"]              

    def get_link(self):
        page = requests.get(self.downloadurl)
        soup = BeautifulSoup(page.content, 'html.parser')        

        links = []
        link = self.baseurl + eval(self.config["find"])        
        return link       

    def download_unzip_file(self, destination, force=False):        
        destination = os.path.join(get_config("directories")["raw"],destination)
        if (os.path.exists(destination) and force is True) | \
            (not os.path.exists(destination)):
            link = self.get_link()        
            print("Downloading data from {url}".format(url=link), end=" ")
            results = urlopen(link)
            zipfile = ZipFile(BytesIO(results.read()))
            zipfile.extractall(destination)
            print("...complete!")
        print("File download complete.")

class Downloader:
    """Downloads files from website when urls are known"""
    def __init__(self, config_section):
        self.baseurl = get_config(config_section)["baseurl"]
        self.filenames = get_config(config_section + "_files")

    def download_files(self, destination, force=False):
        destination = os.path.join(get_config("directories")["raw"],destination)
        if (os.path.exists(destination) and force is True) | (not os.path.exists(destination)):

            for key, filename in self.filenames.items():            
                url = self.baseurl + filename
                print("Downloading data from {url}".format(url=url), end=" ")
                results = requests.get(url)
                zipfile = ZipFile(BytesIO(results.content))
                zipfile.extractall(destination)
                print("...complete!")
        print("File download complete.")



# %%
