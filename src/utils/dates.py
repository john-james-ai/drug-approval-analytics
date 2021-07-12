#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#==============================================================================#
# Project  : Predict-FDA                                                       #
# Version  : 0.1.0                                                             #
# File     : \dates.py                                                         #
# Language : Python 3.9.5                                                      #
# -----------------------------------------------------------------------------#
# Author   : John James                                                        #
# Company  : nov8.ai                                                           #
# Email    : john.james@nov8.ai                                                #
# URL      : https://github.com/john-james-sf/predict-fda                      #
# -----------------------------------------------------------------------------#
# Created  : Saturday, July 10th 2021, 9:07:44 am                              #
# Modified : Saturday, July 10th 2021, 9:44:34 am                              #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#
from datetime import datetime
import unicodedata
# -----------------------------------------------------------------------------#
class Parser:
    def drugs_last_updated(self, text):
        """Takes text containing a date and returns a datetime object"""
        datestring = text.split(':')[1].replace(",", "")
        datestring = unicodedata.normalize("NFKD", datestring)
        datestruct = datestring.split()
        if len(datestruct[1]) == 1:
            datestruct[1] = "0" + datestruct[1]
        dateformat = datestruct[0] + " " + datestruct[1] + " " + datestruct[2]        
        return datetime.strptime(dateformat, "%B %d %Y")
        
