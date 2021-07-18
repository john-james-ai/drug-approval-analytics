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
# Modified : Sunday, July 18th 2021, 8:03:24 am                                #
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


def main():
    webpage = "https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/latest/"    
    response = requests.get(webpage)   
    # Use BeautifulSoup to create searchable html parser
    soup = BeautifulSoup(response.content, 'html.parser')    
    # Get the first table on the webpage
    link = soup.find_all('a', href=re.compile("https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/latest/chembl_28_postgresql.tar.gz"))
    print(link)

if __name__ == "__main__":
    main()   