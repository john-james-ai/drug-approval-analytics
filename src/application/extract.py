#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# =========================================================================== #
# Project  : Drug Approval Analytics                                          #
# Version  : 0.1.0                                                            #
# File     : \src\data\extract.py                                             #
# Language : Python 3.9.5                                                     #
# --------------------------------------------------------------------------  #
# Author   : John James                                                       #
# Company  : nov8.ai                                                          #
# Email    : john.james@nov8.ai                                               #
# URL      : https://github.com/john-james-sf/drug-approval-analytics         #
# --------------------------------------------------------------------------  #
# Created  : Saturday, July 17th 2021, 2:00:13 pm                             #
# Modified : Monday, August 16th 2021, 12:47:43 am                            #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
"""Data source extract module"""
from abc import ABC, abstractmethod
from datetime import datetime
import os
import logging
import requests
import shutil
# from urilib.request import uriopen
# from io import BytesIO
# import unicodedata
# import re
# import json
# from zipfile import ZipFile

from bs4 import BeautifulSoup
# from bs4.element import ResultSet
import pandas as pd

from ..platform.metadata.repository import DataSource, DataSourceEvent
from ..platform.base import Operator
from src.application.config import DBCredentials
# --------------------------------------------------------------------------- #
logger = logging.getLogger(__name__)
