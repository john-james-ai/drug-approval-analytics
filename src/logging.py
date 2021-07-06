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
# Modified : Monday, July 5th 2021, 5:20:17 am                                 #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#
import os
import sys
import types
import logging
import inspect
from logging import Formatter
from logging.handlers import TimedRotatingFileHandler
import functools
from urllib.error import URLError, HTTPError

import psycopg2
from configs.config import Config
# -----------------------------------------------------------------------------#
class Logger:
    def __init__(self, logname):        
        self._logname= logname
        self._logdir = Config().get('directories')['logs']
        self._logfile = os.path.join(self._logdir, logname)
        self._formatter = Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    @property
    def console_handler(self):
        console_handler = logging.StreamHandler(sys.stdout)
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

# -----------------------------------------------------------------------------#
#                             DECORATORS                                       #
# -----------------------------------------------------------------------------#
def logging_decorator(func):
    """Log before and after action."""
    @functools.wraps(func)
    def wrapper_logger(*args, **kwargs):        
        name = '{}'.format(func.__qualname__)
        log_class = Logger(func.__module__)
        logger = log_class.get_logger()
        logger.info("Entering {name}".format(name=name))
        result = func(*args, **kwargs)
        logger.info("Completed {name}".format(name=name))
        return result
    return wrapper_logger

# -----------------------------------------------------------------------------#
def exception_handler(func):
    """Log Exception Decorator    

    A decorator that catches any exceptions thrown by the decorated function and
    logs them along with a traceback that includes every local variable of every
    frame.
    
    Notes:
        Caught exceptions are re-raised.
    Returns:
        A decorator that catches and logs exceptions thrown by decorated functions.
    Raises:
        Nothing. The decorator will re-raise any exception caught by the decorated
        function.

    """

    @functools.wraps(func)
    def wrapper_logger(*args, **kwargs):
        name = '{}'.format(func.__qualname__)
        # Get a logger object
        log_class = Logger(func.__module__)
        logger = log_class.get_logger()

        try:
            return func(*args, **kwargs)

        except (Exception, HTTPError, URLError, psycopg2.DatabaseError) as error:
            # Write Exception
            logger.exception("Exception thrown {}: {}\n ".format(type(error), str(error)))
            
            # iterate through the frames in reverse order so we print the 
            # most recent frame first
            frames = inspect.getinnerframes(sys.exc_info()[2])
            for frame in reversed(frames):                                
                frame_msg = '  File {}, line {}, in {}    {}\n'.format(frame[1], \
                    frame[2], frame[3], frame[4][0].lstrip())
                logger.exception(frame_msg)

                # log local variables of the frame
                for k, v in frame[0].f_locals:
                    variables = "    {} = {}\n".format(k,str(v))
                    logger.exception(variables)
            # Re-raise the exception
            raise
    return wrapper_logger

    
    



