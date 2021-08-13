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
# Modified : Friday, August 13th 2021, 5:15:29 am                             #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
import logging
import math

# --------------------------------------------------------------------------- #
logger = logging.getLogger(__name__)


def start(func):
    text = "Starting {} Tests.".format(func.__class__.__name__)
    linelen = 80
    textlen = len(text)
    n_spaces = math.floor(0.5*(linelen - textlen))
    linebreak1 = linelen*"="
    linebreak2 = "\n\n\n" + " "*n_spaces + text + "\n\n"
    linebreak3 = linelen*"-" + "\n"
    logger.debug("\n")
    logger.debug(linebreak1)
    logger.debug(linebreak2)
    logger.debug(linebreak3)


def end(func):
    text = "Completed {} Tests -> Success!.".format(func.__class__.__name__)
    linelen = 80
    textlen = len(text)
    n_spaces = math.floor(0.5*(linelen - textlen))
    linebreak1 = linelen*"-"
    linebreak2 = "\n\n\n" + " "*n_spaces + text + "\n\n"
    linebreak3 = linelen*"=" + "\n"
    logger.debug("\n")
    logger.debug(linebreak1)
    logger.debug(linebreak2)
    logger.debug(linebreak3)
