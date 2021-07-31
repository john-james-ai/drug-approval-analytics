#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# =========================================================================== #
# Project  : Drug Approval Analytics                                          #
# Version  : 0.1.0                                                            #
# File     : \src\utils\log.py                                                #
# Language : Python 3.9.5                                                     #
# --------------------------------------------------------------------------  #
# Author   : John James                                                       #
# Company  : nov8.ai                                                          #
# Email    : john.james@nov8.ai                                               #
# URL      : https://github.com/john-james-sf/drug-approval-analytics         #
# --------------------------------------------------------------------------  #
# Created  : Wednesday, July 21st 2021, 9:32:12 pm                            #
# Modified : Friday, July 30th 2021, 10:49:38 pm                              #
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
import inspect
import sys
# --------------------------------------------------------------------------- #
LOG_FRAME_TPL = '  File "%s", line %i, in %s\n    %s\n'
log_filepath = 'exceptions.log',
# --------------------------------------------------------------------------- #


def log_to_str(v):
    """ Converts newlines to  newline literals."""
    if isinstance(v, str):
        return ("").join(["'", v.replace('\n', '\\n'), "'"])
    else:
        return str(v).replace('\n', '\\n')


def exception_handler(frame_template: str = LOG_FRAME_TPL,
                      value_to_string: str = log_to_str,
                      log_if: bool = True) -> None:
    """ Exception handler decorator.
    Arguments:

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

        logging.basicConfig(level=logging.DEBUG)
        logger = logging.getLogger(func.__class__.__name__)

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)

        # create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # add formatter to ch
        console_handler.setFormatter(formatter)

        # add ch to logger
        logger.addHandler(console_handler)

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
                    print("\nPRINTING FRAMES")
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
                    logger.error(frame_template %
                                 (frame_info[1], frame_info[2],
                                  frame_info[3],
                                  frame_info[4][0].lstrip()))

                    # log every local variable of the frame
                    for k, v in f_locals.items():
                        print("\nPRINTING VARIABLES")
                        logger.error('    %s = %s\n' %
                                     (k, value_to_string(v)))

                raise

        return wrapper

    return decorator
