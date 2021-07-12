#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#==============================================================================#
# Project  : Predict-FDA                                                       #
# Version  : 0.1.0                                                             #
# File     : \scrape.py                                                        #
# Language : Python 3.9.5                                                      #
# -----------------------------------------------------------------------------#
# Author   : John James                                                        #
# Company  : nov8.ai                                                           #
# Email    : john.james@nov8.ai                                                #
# URL      : https://github.com/john-james-sf/predict-fda                      #
# -----------------------------------------------------------------------------#
# Created  : Saturday, July 10th 2021, 6:53:42 am                              #
# Modified : Sunday, July 11th 2021, 11:43:21 pm                               #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#
#%%
from datetime import datetime
import os
import unicodedata
import re
import requests
from bs4 import BeautifulSoup
from src.utils.dates import Parser


def main():
    webpage = "https://aact.ctti-clinicaltrials.org/snapshots"
    baseurl = "https://aact.ctti-clinicaltrials.org"
    response = requests.get(webpage)   
    # Use BeautifulSoup to create searchable html parser
    soup = BeautifulSoup(response.content, 'html.parser')    
    # Get the first table on the webpage
    table = soup.findChildren('table')[0]            
    # Get the first row
    row = table.find_all('tr')[0]    
    # Get the second column which contains the date the file was created.
    date_string = row.find_all('td')[1].text
    # Convert the text to datetime object
    print(datetime.strptime(date_string, "%m/%d/%Y"))

if __name__ == "__main__":
    main()   