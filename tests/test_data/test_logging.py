#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#==============================================================================#
# Project  : Predict-FDA                                                       #
# Version  : 0.1.0                                                             #
# File     : \test_logging.py                                                  #
# Language : Python 3.9.5                                                      #
# -----------------------------------------------------------------------------#
# Author   : John James                                                        #
# Company  : nov8.ai                                                           #
# Email    : john.james@nov8.ai                                                #
# URL      : https://github.com/john-james-sf/predict-fda                      #
# -----------------------------------------------------------------------------#
# Created  : Wednesday, June 30th 2021, 10:25:41 am                            #
# Modified : Wednesday, June 30th 2021, 10:40:39 am                            #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#
import pytest

from src.logging import Logger

@pytest.mark.logging
class LoggingTests:

    def test_logging(self):        
        logo = Logger(__name__)
        logger = logo.get_logger()
        logger.info("This is an FYI")
        logger.debug("Uh oh, we have a bug")
        logger.warning("Consider yourself on notice")
        logger.error("Ok, this is an ERROR")





