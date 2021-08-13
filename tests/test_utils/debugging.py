#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# =========================================================================== #
# Project  : Drug Approval Analytics                                          #
# Version  : 0.1.0                                                            #
# File     : \src\utils\debugging.py                                          #
# Language : Python 3.9.5                                                     #
# --------------------------------------------------------------------------  #
# Author   : John James                                                       #
# Company  : nov8.ai                                                          #
# Email    : john.james@nov8.ai                                               #
# URL      : https://github.com/john-james-sf/drug-approval-analytics         #
# --------------------------------------------------------------------------  #
# Created  : Friday, July 30th 2021, 1:03:54 pm                               #
# Modified : Friday, August 13th 2021, 3:21:37 am                             #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
"""Various debugging helper functions and decorators."""
import functools
import logging
# --------------------------------------------------------------------------- #
logger = logging.getLogger(__name__)


def announce(func):
    """Print the function signature and return value"""
    @functools.wraps(func)
    def wrapper_debug(*args, **kwargs):
        enter = "\n\n\t\t\tEntering {}\n\n".format(func.__qualname__)
        leave = "\n\n\t\t\tLeaving {}\n\n".format(func.__qualname__)
        logger.debug(enter)
        value = func(*args, **kwargs)
        logger.debug(leave)
        return value
    return wrapper_debug
