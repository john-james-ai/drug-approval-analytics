#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#==============================================================================#
# Project  : Predict-FDA                                                       #
# Version  : 0.1.0                                                             #
# File     : \data.py                                                          #
# Language : Python 3.9.5                                                      #
# -----------------------------------------------------------------------------#
# Author   : John James                                                        #
# Company  : nov8.ai                                                           #
# Email    : john.james@nov8.ai                                                #
# URL      : https://github.com/john-james-sf/predict-fda                      #
# -----------------------------------------------------------------------------#
# Created  : Wednesday, June 23rd 2021, 12:44:42 pm                            #
# Modified : Wednesday, June 23rd 2021, 7:21:06 pm                             #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#
#%%
from approval.database.aact_db import AACTDao
from approval.database.sql import Queryator
qg = Queryator()
query = qg.columns('studies')

aact = AACTDao()
columns = aact.read_table(query)
print(columns)


# %%
