#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#==============================================================================#
# Project  : Predict-FDA                                                       #
# Version  : 0.1.0                                                             #
# File     : \pipeline.py                                                      #
# Language : Python 3.9.5                                                      #
# -----------------------------------------------------------------------------#
# Author   : John James                                                        #
# Company  : nov8.ai                                                           #
# Email    : john.james@nov8.ai                                                #
# URL      : https://github.com/john-james-sf/predict-fda                      #
# -----------------------------------------------------------------------------#
# Created  : Friday, July 9th 2021, 7:32:05 pm                                 #
# Modified : Sunday, July 11th 2021, 11:30:56 pm                               #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#
"""Contains Pipeline and base classes for Pipeline steps i.e. Tasks objects."""
from abc import ABC, abstractmethod

from sqlalchemy.ext.hybrid import hybrid_property

from approval.logging import log_step
# -----------------------------------------------------------------------------#
class Pipeline:
    """Pipeline object for processing data from the data like to the data warehouse."""
    
    def __init__(self, name, dataobject):
        self.name = name
        self._dataobject = dataobject
        self._steps = []

    def get_steps(self):
        return self._steps        

    def add_step(self, step):
        self._steps.append(step)

    def clear_steps(self):
        self._steps = []        

    def execute(self):
        for step in self._steps:
            self._dataobject = step.execute(dataobject=self._dataobject)

        return self._dataobject

    @property
    def stepnames(self):
        return [step.name for step in self._steps]

# -----------------------------------------------------------------------------#
class PipelineStep(ABC):
    """Base class for objects representing a step in the Pipeline.

    Arguments
    ---------
    name str: The name of the PipelineStep object
    input_do DataObject: The DataObject serving as input to this step.
    output_do DataObject: The DataObject produced by the step
    
    """

    def __init__(self, name, seq, *args, **kwargs):
        self._name = name
        self._seq = seq

    def __str__(self):
        return "{classname}({name}\n\t{seq})".format(
            classname=self.__class__.__name__,
            name=str(self._name),
            seq=str(self._seq)
        )

    def __repr__(self):
        return "{classname}({name}\n\t{seq})".format(
            classname=self.__class__.__name__,
            name=repr(self._name),
            seq=repr(self._seq)
        )

    @property
    def name(self):
        return self._name

    @property
    def seq(self):
        return self._seq        
    
    @abstractmethod
    def execute(self, dataobject):
        pass
