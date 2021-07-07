#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#==============================================================================#
# Project  : Predict-FDA                                                       #
# Version  : 0.1.0                                                             #
# File     : \files.py                                                         #
# Language : Python 3.9.5                                                      #
# -----------------------------------------------------------------------------#
# Author   : John James                                                        #
# Company  : nov8.ai                                                           #
# Email    : john.james@nov8.ai                                                #
# URL      : https://github.com/john-james-sf/predict-fda                      #
# -----------------------------------------------------------------------------#
# Created  : Monday, July 5th 2021, 3:35:51 pm                                 #
# Modified : Tuesday, July 6th 2021, 7:04:51 pm                                #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#
from datetime import datetime
import os
import time
# -----------------------------------------------------------------------------#
def get_date_modified(filepath):
    return time.strftime("%Y-%m-%d", time.localtime(os.path.getmtime(filepath)))

def modified_today(filepath):
    today = datetime.now().strftime("%Y-%m-%d")
    modified = time.strftime("%Y-%m-%d", time.localtime(os.path.getmtime(filepath)))
    return today == modified


def numfiles(dirpath):
    return len(os.listdir(dirpath))
