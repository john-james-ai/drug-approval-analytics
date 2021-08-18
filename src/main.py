#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# =========================================================================== #
# Project  : Drug Approval Analytics                                          #
# Version  : 0.1.0                                                            #
# File     : \src\main.py                                                     #
# Language : Python 3.9.5                                                     #
# --------------------------------------------------------------------------  #
# Author   : John James                                                       #
# Company  : nov8.ai                                                          #
# Email    : john.james@nov8.ai                                               #
# URL      : https://github.com/john-james-sf/drug-approval-analytics         #
# --------------------------------------------------------------------------  #
# Created  : Thursday, June 24th 2021, 11:54:46 am                            #
# Modified : Tuesday, August 17th 2021, 8:37:53 pm                            #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
# %%
from pathlib import Path
import logging

from src.infrastructure.setup import PlatformBuilder
from src.infrastructure.data.config import pg_pg_login, DBCredentials

# import click


def setup_database(dbname: str, credentials: DBCredentials) -> None:
    print(credentials)
    mdata = "platform/metadata/metadata_table_create.sql"
    dsources = "..data/metadata/datasources.csv"
    builder = PlatformBuilder(pg_pg_login)
    builder.build_user(credentials)
    builder.build_database(dbname)
    builder.build_metadata(mdata)
    builder.build_datasources(dsources)


def get_user_config():

    dbname = 'rx2m'
    # username = input(
    #     "Welcome. Please enter your credentials in the config file and enter\
    #      your username? ['j2']"
    # )
    config = DBCredentials()
    credentials = config.get_config('j2')

    setup_database(dbname, credentials)


# @click.command()
# @click.argument('input_filepath', type=click.Path(exists=True))
# @click.argument('output_filepath', type=click.Path())
def main():
    """ Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """
    logger = logging.getLogger(__name__)
    logger.info('making final data set from raw data')
    get_user_config()


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    main()
