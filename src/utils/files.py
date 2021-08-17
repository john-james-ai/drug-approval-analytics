#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# =========================================================================== #
# Project  : Drug Approval Analytics                                          #
# Version  : 0.1.0                                                            #
# File     : \src\utils\files.py                                              #
# Language : Python 3.9.5                                                     #
# --------------------------------------------------------------------------  #
# Author   : John James                                                       #
# Company  : nov8.ai                                                          #
# Email    : john.james@nov8.ai                                               #
# URL      : https://github.com/john-james-sf/drug-approval-analytics         #
# --------------------------------------------------------------------------  #
# Created  : Monday, July 5th 2021, 3:35:51 pm                                #
# Modified : Tuesday, August 17th 2021, 6:18:19 am                            #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#==============================================================================#
# Project  : Predict-FDA                                                       #
# Version  : 0.1.0                                                             #
# File     : \files.py                                                         #
# Language : Python 3.9.5                                                      #
# -----------------------------------------------------------------------------#
# Author   : John James                                                        #
# Company  : nov8.ai                                                           #
# Email    : john.james@nov8.ai                                                #
# URL      : https://github.com/john-james-sf/predict-fda                      #
# -----------------------------------------------------------------------------#
# Created  : Monday, July 5th 2021, 3:35:51 pm                                 #
# Modified : Saturday, July 10th 2021, 5:59:12 pm                              #
# Modifier : John James (john.james@nov8.ai)                                   #
# -----------------------------------------------------------------------------#
# License  : BSD 3-clause "New" or "Revised" License                           #
# Copyright: (c) 2021 nov8.ai                                                  #
#==============================================================================#
from datetime import datetime
import os
import time
import locale
# -----------------------------------------------------------------------------#


def get_date_modified(filepath):
    return time.strftime("%Y-%m-%d", time.localtime(os.path.getmtime(filepath)))


def modified_today(filepath):
    today = datetime.now().strftime("%Y-%m-%d")
    modified = time.strftime(
        "%Y-%m-%d MM", time.localtime(os.path.getmtime(filepath)))
    return today == modified


def numfiles(dirpath):
    return len(os.listdir(dirpath))


def get_file_metadata(directory, filename):
    # Directory shouldn't end with backslash, i.e. "E:\Images\Paris"
    # filename must include extension, i.e. "PID manual.pdf"
    # Returns dictionary containing file metadata.
    locale.setlocale(locale.LC_ALL, 'en_US')
    filepath = os.path.join(directory, filename)

    metadata = {}
    metadata['Directory'] = directory
    metadata['Filename'] = filename
    metadata['Filesize (Bytes)'] = locale.format(
        "%d", os.path.getsize(filepath), grouping=True)
    metadata['Created'] = time.strftime(
        "%Y-%m-%d %H:%M:%S", time.localtime(os.path.getctime(filepath)))
    metadata['Updated'] = time.strftime(
        "%Y-%m-%d %H:%M:%S", time.localtime(os.path.getmtime(filepath)))
    return metadata


def string_replace(filepath, from_str, to_str):
    # open file in read mode
    file = open(filepath, "r")
    replaced_content = ""
    # looping through the file
    for line in file:

        # stripping line break
        line = line.strip()
        # replacing the texts
        new_line = line.replace(from_str, to_str)
        # concatenate the new string and add an end-line break
        replaced_content = replaced_content + new_line + "\n"

    # close the file
    file.close()
    # Open file in write mode
    write_file = open(filepath, "w")
    # overwriting the old file contents with the new/replaced content
    write_file.write(replaced_content)
    # close the file
    write_file.close()
