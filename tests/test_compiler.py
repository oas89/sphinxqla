# -*- coding: utf-8 -*-
from unittest import TestCase

from sqlalchemy import select
from sqlalchemy.sql import table, column

from .. import sphinxql
from ..sphinxtypes import *


index = table('index',
              column('id', Unsigned),
              column('content', Fulltext),
              column('multi', Multi))


test = table('test',
             column('id', Unsigned))


types = table('types',
              column('unsigned', Unsigned),
              column('float_', Float),
              column('timestamp', Timestamp),
              column('multi', Multi),
              column('string', String),
              column('fulltext', Fulltext))


dialect = sphinxql.Dialect()
compiled = lambda stmt: str(stmt.compile(dialect=dialect))


class TestCompiler(TestCase):

    def test_regression(self):
        self.assertEqual(compiled(
            index.select()),
                         "SELECT id, content, multi \n"
                         "FROM index")
        self.assertEqual(compiled(
            index.insert()),
                         "INSERT INTO index (id, content, multi) "
                         "VALUES (%s, %s, %s)")

    def test_match_function(self):
        self.assertEqual(compiled(
            index.select().where(sphinxql.match('test'))),
                         "SELECT id, content, multi \n"
                         "FROM index \n"
                         "WHERE MATCH(%s)")

    def test_insert_multi(self):
        self.assertEqual(compiled(
            index.insert().values(id=1, content='test', multi=multi([1,2]))),
                         "INSERT INTO index (id, content, multi) "
                         "VALUES (%s, %s, (%s, %s))")

    def test_limit_clause(self):
        self.assertEqual(compiled(
            test.select().limit(10)),
                         "SELECT id \n"
                         "FROM test \n"
                         "LIMIT %s")
        self.assertEqual(compiled(
            test.select().limit(10).offset(20)),
                         "SELECT id \n"
                         "FROM test \n"
                         "LIMIT %s, %s")
        self.assertEqual(compiled(
            test.select().offset(20)),
                         "SELECT id \n"
                         "FROM test")


class TestTypes(TestCase):

    def test_multi(self):
          self.assertEqual(compiled(types.c.multi.in_([1])),
                           'multi IN (%s)')
          self.assertEqual(compiled(types.c.multi.in_([1, 2])),
                           'multi IN (%s, %s)')
