#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# =========================================================================== #
# Project  : Drug Approval Analytics                                          #
# Version  : 0.1.0                                                            #
# File     : \tests\test_platform\test_config.py                              #
# Language : Python 3.9.5                                                     #
# --------------------------------------------------------------------------  #
# Author   : John James                                                       #
# Company  : nov8.ai                                                          #
# Email    : john.james@nov8.ai                                               #
# URL      : https://github.com/john-james-sf/drug-approval-analytics         #
# --------------------------------------------------------------------------  #
# Created  : Tuesday, August 10th 2021, 7:30:44 pm                            #
# Modified : Tuesday, August 10th 2021, 8:09:06 pm                            #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
import pytest

from src.platform.config import DBCredentials
# --------------------------------------------------------------------------- #


@pytest.mark.credentials
class DBCredentialsTests:

    @pytest.mark.credentials
    def test_set_credentials(self):
        name = 'test_user'
        user = 'test_username'
        password = 'test_user_password'
        dbname = "test_db"
        host = 'some_host'
        port = 2398
        credentials = DBCredentials()
        credentials.create(name=name, user=user, password=password,
                           dbname=dbname, host=host, port=port)
        credentials.load(name)
        # Test object attributes
        assert user == credentials.user, "DBCredentials ValueError"
        assert dbname == credentials.dbname, "DBCredentials ValueError"
        assert host == credentials.host, "DBCredentials ValueError"
        assert password == credentials.password, "DBCredentials ValueError"
        assert port == int(credentials.port), "DBCredentials ValueError"

        # Test dictionary casting
        assert user == credentials['user'], "DBCredentials ValueError"
        assert dbname == credentials['dbname'], "DBCredentials ValueError"
        assert host == credentials['host'], "DBCredentials ValueError"
        assert password == credentials['password'], "DBCredentials ValueError"
        assert port == int(credentials['port']), "DBCredentials ValueError"
        assert isinstance(dict(credentials), dict), "DBCredentials, TypeError"
