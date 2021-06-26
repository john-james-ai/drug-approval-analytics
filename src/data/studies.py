#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#==============================================================================#
# Project  : Predict-FDA                                                       #
# Version  : 0.1.0                                                             #
# File     : \studies.py                                                       #
# Language : Python 3.9.5                                                      #
# -----------------------------------------------------------------------------#
# Author   : John James                                                        #
# Company  : nov8.ai                                                           #
# Email    : john.james@nov8.ai                                                #
# URL      : https://github.com/john-james-sf/predict-fda                      #
# -----------------------------------------------------------------------------#
# Created  : Thursday, June 24th 2021, 2:41:42 pm                              #
# Modified : Thursday, June 24th 2021, 4:20:11 pm                              #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#
#%%
import pandas as pd
pd.set_option("display.max_rows",500)

filename = "./data/raw/studies.csv"
df = pd.read_csv(filename, low_memory=False, index_col=0)
dims = df.shape
n_trials = df["nct_id"].nunique()
dtypes = df.dtypes
print(dims[0])
print(dims[1])
print(n_trials)
print(df.head())
print(df.describe())
print(dtypes)
# %%
