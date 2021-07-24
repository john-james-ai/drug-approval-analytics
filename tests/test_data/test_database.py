#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# =========================================================================== #
# Project  : Drug Approval Analytics                                          #
# Version  : 0.1.0                                                            #
# File     : \tests\test_data\test_database.py                                #
# Language : Python 3.9.5                                                     #
# --------------------------------------------------------------------------  #
# Author   : John James                                                       #
# Company  : nov8.ai                                                          #
# Email    : john.james@nov8.ai                                               #
# URL      : https://github.com/john-james-sf/drug-approval-analytics         #
# --------------------------------------------------------------------------  #
# Created  : Sunday, July 4th 2021, 6:46:35 pm                                #
# Modified : Thursday, July 22nd 2021, 1:01:27 pm                             #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
# %%
import pytest
import pandas as pd
from src.platform.repository import RepoSetup
from src.data.database.admin import DBAdmin
from src.data.database.access import DBDao
# -----------------------------------------------------------------------------#


@pytest.mark.dbadmin
class DBAdminTests:

    def __init__(self):
        dbadmin = DBAdmin()
        dbadmin.drop_database('metadata')

    def test_database_exists(self):
        dbadmin = DBAdmin()
        version = dbadmin.database_exists('AACT')
        assert version is not None, "Error: Database version not returned"

    def test_create_db(self):
        dbadmin = DBAdmin()
        dbadmin.create_database('metadata')
        version = dbadmin.database_exists('metadata')
        assert version is not None, "Error: Database not created."

    # TODO
    def test_drop_db(self):
        pass

    # TODO
    def test_backup_db(self):
        pass

    # TODO
    def test_restore_db(self):
        pass

    def test_create_table(self):
        # Create table
        dbadmin = DBAdmin()
        sql_command = (
            """
            CREATE TABLE attributes (
                id SERIAL PRIMARY KEY,
                attribute VARCHAR(120) NOT NULL,
                entity VARCHAR(48) NOT NULL,
                datatype VARCHAR(48) NOT NULL
            )
            """
        )
        dbadmin.create_table('metadata', 'attributes', sql_command)

        # Get list of tables from Dao
        dao = DBDao('metadata')
        assert 'attributes' in dao.get_tables(), "Error: Table not in schema."


class DBDaoTests:

    def __init__(self, filepath):
        self._data = pd.read_csv(filepath)
        self._dao = DBDao('metadata')

    def test_load(self):
        self._dao.load(table='attributes', df=self._data)

    def test_read_table(self):
        df = self._dao.read_table('attributes')
        assert df.shape[0] > 100, "Error: Expected shape[0] > 100, Actual = {}.".format(
            df.shape[0])
        assert df.shape[1] == 4, "Error: Expected shape[1] = 4, Actual = {}.".format(
            df.shape[1])

    def test_update(self):
        self._dao.update('attributes', 'datatype',
                         'Date', 'attribute', 'phase')
        df = self._dao.read_table(
            'attributes', 'attribute', 'phase').reset_index()
        assert df[df["attribute"] == "phase"]['datatype'].any(
        ) == 'Date', "Error: Update didn't work."

    def test_read(self):
        df = self._dao.read('attributes', 'attribute', 'phase')
        assert df['datatype'].any() == 'Date', "Error, read didn't work."

    def test_delete(self):
        self._dao.delete('attributes', 'attribute', 'phase')
        df = self._dao.read('attributes', 'attribute', 'phase')
        assert df.shape[0] == 0, "Error, delete didn't work."

    def test_columns(self):
        columns = self._dao.get_columns('attributes', metadata=False)
        assert len(columns) == 4, "Error, Number of columns is incorrect"
        assert 'datatype' in columns, "Error, Columns didn't work"

        columns = self._dao.get_columns('attributes', metadata=True)
        assert len(columns) > 4, "Error, Number of columns is incorrect"
        assert 'scope_schema' in columns, "Error, Columns didn't work"

    def test_teardown(self):
        del(self._dao)


class RepoDBTests:

    @pytest.mark.repo
    def test_repo_setup(self):
        setup = RepoSetup()
        config = setup.execute()
        print(config)


def main():
    # dbadmin = DBAdminTests()
    # dbadmin.test_database_exists()
    # dbadmin.test_create_db()
    # dbadmin.test_create_table()

    # filepath = "./data/metadata/aact_metadata.csv"
    # dbao = DBDaoTests(filepath)
    # dbao.test_load()
    # dbao.test_read_table()
    # dbao.test_update()
    # dbao.test_read()
    # dbao.test_delete()
    # dbao.test_columns()
    # logger.info("DataBase Tests Complete")

    rt = RepoDBTests()
    rt.test_repo_setup()


if __name__ == "__main__":
    main()
# %%
