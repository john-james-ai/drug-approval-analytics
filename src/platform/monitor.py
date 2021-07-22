#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# =========================================================================== #
# Project  : Drug Approval Analytics                                          #
# Version  : 0.1.0                                                            #
# File     : \src\platform\monitor.py                                         #
# Language : Python 3.9.5                                                     #
# --------------------------------------------------------------------------  #
# Author   : John James                                                       #
# Company  : nov8.ai                                                          #
# Email    : john.james@nov8.ai                                               #
# URL      : https://github.com/john-james-sf/drug-approval-analytics         #
# --------------------------------------------------------------------------  #
# Created  : Wednesday, July 21st 2021, 8:05:40 pm                            #
# Modified : Wednesday, July 21st 2021, 10:19:48 pm                           #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
"""This module provides a exception handler decorator for pipeline objects.

This exception handler decorator catches any exception thrown by the
decorated function and logs it along with a traceback that includes every
local variable of every frame.

"""
import logging
import functools
import types
import inspect
import sys
# --------------------------------------------------------------------------- #
logging.setLevel(logging.INFO)
LOG_FRAME_TPL = '  File "%s", line %i, in %s\n    %s\n'
# --------------------------------------------------------------------------- #


def log_to_str(v):
    """ Converts newlines to  newline literals."""
    if isinstance(v, types.StringType):
        return ["'", v.replace('\n', '\\n'), "'"].join('')
    else:
        return str(v).replace('\n', '\\n')


def exception_handler(log_filepath: str = 'exceptions.log',
                      frame_template: str = LOG_FRAME_TPL,
                      value_to_string: str = log_to_str,
                      log_if: bool = True) -> None:
    """ Exception handler decorator.
    Arguments:
        log_filepath (string): The log file path. Defaults to
        'exceptions.log'.

        frame_template (string): a format string used to format frame
        information. The following format arguments will be used when
        printing a frame:
            - File (string): The name of the file to which the frame belongs.
            - line (int): The line number on which the frame was created.
            - in (string) The name of the function to which the frame belongs.
            - func (string) The python code of the line where the frame was
            created.

            The default frame template outputs frame info as it appears in
            normal python tracebacks.

        value_to_string (function): a function that converts arbitrary
            values to strings. The default converter calls str() and
            replaces newlines with the newline literal. This
            function MUST NOT THROW.

        log_if (bool): Not used in this package

    Returns:
        A decorator that catches and logs exceptions thrown by decorated
        functions.

    Raises:
        Nothing. The decorator will re-raise any exception caught by the
        decorated function.

    Notes:
        Caught exceptions are re-raised.

    Reference:
        This module was motivated by the following gist:
            https://gist.github.com/diosmosis/1148066

  """

    def decorator(func):

        logger = logging.basicConfig(filepath=log_filepath, encoding='utf-8')

        @functools.wraps(func)
        def wrapper(*args, **kwds):

            try:
                return func(*args, **kwds)

            except Exception as error:

                # log exception information first in case something fails
                # below
                logger.error('Exception thrown, %s: %s\n' % (type(error),
                                                             str(error)))

                # iterate through the frames in reverse order so we print
                # the most recent frame first
                frames = inspect.getinnerframes(sys.exc_info()[2])
                for frame_info in reversed(frames):
                    f_locals = frame_info[0].f_locals

                    # if there's a local variable named
                    # '__lgw_marker_local__', we assume the frame is
                    # from a call of this function, 'wrapper', and
                    # we skip it. Printing these frames won't help
                    # determine the cause of an exception, so skipping
                    # it reduces clutter.
                    if '__lgw_marker_local__' in f_locals:
                        continue

                    # log the frame information
                    logging.error(frame_template %
                                  (frame_info[1], frame_info[2],
                                   frame_info[3],
                                   frame_info[4][0].lstrip()))

                    # log every local variable of the frame
                    for k, v in f_locals.items():
                        logger.error('    %s = %s\n' %
                                     (k, value_to_string(v)))

                raise

        return wrapper

    return decorator
