#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#==============================================================================#
# Project  : Predict-FDA                                                       #
# Version  : 0.1.0                                                             #
# File     : \logging.py                                                       #
# Language : Python 3.9.5                                                      #
# -----------------------------------------------------------------------------#
# Author   : John James                                                        #
# Company  : nov8.ai                                                           #
# Email    : john.james@nov8.ai                                                #
# URL      : https://github.com/john-james-sf/predict-fda                      #
# -----------------------------------------------------------------------------#
# Created  : Wednesday, June 30th 2021, 9:58:34 am                             #
# Modified : Wednesday, June 30th 2021, 10:41:20 am                            #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#
import logging, os, sys
from logging import Formatter
from logging.handlers import TimedRotatingFileHandler

from configs.config import Config
# -----------------------------------------------------------------------------#
class Logger:
    def __init__(self, logname):        
        self._logname= logname
        self._logdir = Config().get_config('basedirectories')['logs']
        self._logfile = os.path.join(self._logdir, logname)
        self._formatter = Formatter("%(asctime)s — %(name)s — %(levelname)s — %(message)s")

    @property
    def console_handler(self):
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(self._formatter)
        return console_handler

    @property
    def file_handler(self):
        file_handler = TimedRotatingFileHandler(self._logfile, when="midnight")
        file_handler.setFormatter(self._formatter)
        return file_handler
    
    def get_logger(self):
        logger = logging.getLogger(self._logname)
        logger.setLevel(logging.DEBUG)
        logger.addHandler(self.console_handler)
        logger.addHandler(self.file_handler)
        logger.propagate = False
        return logger        

