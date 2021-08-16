#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# =========================================================================== #
# Project  : Drug Approval Analytics                                          #
# Version  : 0.1.0                                                            #
# File     : \src\infrastructure\extractors.py                                #
# Language : Python 3.9.5                                                     #
# --------------------------------------------------------------------------  #
# Author   : John James                                                       #
# Company  : nov8.ai                                                          #
# Email    : john.james@nov8.ai                                               #
# URL      : https://github.com/john-james-sf/drug-approval-analytics         #
# --------------------------------------------------------------------------  #
# Created  : Sunday, August 15th 2021, 11:23:10 am                            #
# Modified : Sunday, August 15th 2021, 9:10:30 pm                             #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #


# --------------------------------------------------------------------------- #
#                              Extractors                                     #
# --------------------------------------------------------------------------- #


class ZipExtractor(Operator):
    """Downloads and unpacks zipfiles."""

    def __init__(self, task_id: str, uris: str, destination: str):
        super(ZipExtractor, self).__init__(task_id)
        self._uris = uris
        self._destination = destination

    def download(self, context=None):
        if not os.path.exists(self._destination):
            os.makedirs(self._destination)

        if isinstance(self._uris, str):
            self._uris = [self._uris]

        for uri in self._uris:
            msg = "Download of data from {} started at {}".format(
                uri, datetime.now())
            logger.info(msg)
            results = requests.get(uri)
            zipfile = ZipFile(BytesIO(results.content))
            zipfile.extractall(self._destination)
            msg = "Download of data from {} completed at {}".format(
                uri, datetime.now())
            logger.info(msg)
        msg = "Download completed at {}".format(datetime.now())
        logger.info("Download complete.")
