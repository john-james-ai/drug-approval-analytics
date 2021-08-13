#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# =========================================================================== #
# Project  : Drug Approval Analytics                                          #
# Version  : 0.1.0                                                            #
# File     : \src\lab\dynamic_sql.py                                          #
# Language : Python 3.9.5                                                     #
# --------------------------------------------------------------------------  #
# Author   : John James                                                       #
# Company  : nov8.ai                                                          #
# Email    : john.james@nov8.ai                                               #
# URL      : https://github.com/john-james-sf/drug-approval-analytics         #
# --------------------------------------------------------------------------  #
# Created  : Saturday, July 24th 2021, 7:52:10 pm                             #
# Modified : Friday, August 13th 2021, 2:09:21 am                             #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
# Reference: https://stackoverflow.com/questions/56570952/creating-dynamically-typed-tables-using-psycopg2s-built-in-formatting
"""
I was also having so much trouble with this aspect. sql.Identifier is for
double-quoted, well, SQL Identifiers which the datatypes (INTEGER, TEXT, etc.)
are not. Looks like just making it plain SQL does the trick.

N.B. In your code, you should have pre-defined columns tuples and not
expose their definition to the front-end. This is also why tuples are
useful here as they are immutable.
"""
import psycopg2.sql as sql


def create(name, columns):
    # name = "mytable"
    # columns = (("col1", "TEXT"), ("col2", "INTEGER"), ...)
    fields = []
    for col in columns:
        fields.append(sql.SQL("{} {}").format(
            sql.Identifier(col[0]), sql.SQL(col[1])))

    query = sql.SQL("CREATE TABLE {tbl_name} ( {fields} );").format(
        tbl_name=sql.Identifier(name),
        fields=sql.SQL(', ').join(fields)
    )
    # CREATE TABLE "mytable" ( "col1" TEXT, "col2" INTEGER );
    # print(query.as_string(conn))
    # Get cursor and execute...

# --------------------------------------------------------------------------- #


class User:
    def __init__(self, email, first_name, last_name, id=None):
        self.email = email
        self.first_name = first_name
        self.last_name = last_name
        self.id = id

    def __repr__(self):
        return "<User {}>".format(self.email)

    def save_to_db(self):
        # This is creating a new connection pool every time! Very expensive...
        with CursorFromConnectionPool() as cursor:
            cursor.execute('INSERT INTO users (email, first_name, last_name) VALUES (%s, %s, %s)',
                           (self.email, self.first_name, self.last_name))

    @classmethod
    def load_from_db_by_email(cls, email):
        with CursorFromConnectionPool() as cursor:
            # Note the (email,) to make it a tuple!
            cursor.execute('SELECT * FROM users WHERE email=%s', (email,))
            user_data = cursor.fetchone()
            return cls(email=user_data[1], first_name=user_data[2], last_name=user_data[3], id=user_data[0])


>> > names = ['foo', 'bar', 'baz']

>> > q1 = sql.SQL("insert into table ({}) values ({})").format(
    ...     sql.SQL(', ').join(map(sql.Identifier, names)),
    ...     sql.SQL(', ').join(sql.Placeholder() * len(names)))
>> > print(q1.as_string(conn))
insert into table ("foo", "bar", "baz") values ( % s, % s, % s)

>> > q2 = sql.SQL("insert into table ({}) values ({})").format(
    ...     sql.SQL(', ').join(map(sql.Identifier, names)),
    ...     sql.SQL(', ').join(map(sql.Placeholder, names)))
>> > print(q2.as_string(conn))
insert into table ("foo", "bar", "baz") values ( % (foo)s, % (bar)s, % (baz)s)
