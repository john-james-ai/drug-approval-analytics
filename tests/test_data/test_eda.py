#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#==============================================================================#
# Project  : Drug Approval Analytics                                           #
# Version  : 0.1.0                                                             #
# File     : \tests\test_data\test_eda.py                                      #
# Language : Python 3.9.5                                                      #
# -----------------------------------------------------------------------------#
# Author   : John James                                                        #
# Company  : nov8.ai                                                           #
# Email    : john.james@nov8.ai                                                #
# URL      : https://github.com/john-james-sf/drug-approval-analytics          #
# -----------------------------------------------------------------------------#
# Created  : Tuesday, July 20th 2021, 6:12:48 am                               #
# Modified : Tuesday, July 20th 2021, 12:56:03 pm                              #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#
#%%
import pytest
import logging
import pandas as pd
from src.data.eda import Explorer
# -----------------------------------------------------------------------------#
logging.basicConfig(level=logging.NOTSET)
logger = logging.getLogger(__name__)
# -----------------------------------------------------------------------------#
class ExplorerTests:

    @pytest.mark.eda
    def test_profile(self):
        filepath = "./data/staged/aact/studies.csv"
        df = pd.read_csv(filepath, low_memory=False)
        eda = Explorer(df)
        profile = eda.profile()
        
        print(profile)

def main():
    et = ExplorerTests()
    et.test_profile()

if __name__ == '__main__':
    main()