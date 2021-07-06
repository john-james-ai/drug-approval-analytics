#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#==============================================================================#
# Project  : Predict-FDA                                                       #
# Version  : 0.1.0                                                             #
# File     : \time.py                                                          #
# Language : Python 3.9.5                                                      #
# -----------------------------------------------------------------------------#
# Author   : John James                                                        #
# Company  : nov8.ai                                                           #
# Email    : john.james@nov8.ai                                                #
# URL      : https://github.com/john-james-sf/predict-fda                      #
# -----------------------------------------------------------------------------#
# Created  : Sunday, July 4th 2021, 12:09:31 pm                                #
# Modified : Monday, July 5th 2021, 3:52:25 pm                                 #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#
#%%
import time
from datetime import datetime, date
import os

filename = "./data/downloaded/aact/postgres_data.dmp"
assert os.path.exists(filename)
print(datetime.fromtimestamp(os.path.getmtime(filename)))
print(datetime.fromtimestamp(time.time()))


# %%
