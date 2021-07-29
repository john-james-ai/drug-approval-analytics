#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# =========================================================================== #
# Project  : Drug Approval Analytics                                          #
# Version  : 0.1.0                                                            #
# File     : \tests\test_utils\print.py                                       #
# Language : Python 3.9.5                                                     #
# --------------------------------------------------------------------------  #
# Author   : John James                                                       #
# Company  : nov8.ai                                                          #
# Email    : john.james@nov8.ai                                               #
# URL      : https://github.com/john-james-sf/drug-approval-analytics         #
# --------------------------------------------------------------------------  #
# Created  : Thursday, July 29th 2021, 12:00:27 am                            #
# Modified : Thursday, July 29th 2021, 12:45:06 am                            #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
import math


def start(func):
    text = "Starting {} Tests.".format(func.__class__.__name__)
    linelen = 80
    textlen = len(text)
    n_spaces = math.floor(0.5*(linelen - textlen))
    print("\n")
    print(linelen*"=")
    print(" "*n_spaces, text)
    print(linelen*"-", "\n")


def end(func):
    text = "Completed {} Tests -> Success!.".format(func.__class__.__name__)
    linelen = 80
    textlen = len(text)
    n_spaces = math.floor(0.5*(linelen - textlen))
    print(linelen*"-", "\n")
    print(" "*n_spaces, text)
    print(linelen*"=", "\n")
