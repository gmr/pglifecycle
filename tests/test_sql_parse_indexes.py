import unittest

import pglast.parser

from pglifecycle import sql_parse


class TestCase(unittest.TestCase):

    def test_simple(self):
        sql = 'CREATE INDEX foo ON bar (baz)'
        expectation = {
            'columns': [{'name': 'baz', 'null_order': None, 'order': None}],
            'condition': None,
            'name': 'foo',
            'options': {},
            'relation': {'inh': True,
                         'location': 20,
                         'relname': 'bar',
                         'relpersistence': 'p'},
            'tablespace': None,
            'type': 'btree',
            'unique': False}
        self.assertDictEqual(sql_parse.parse(sql), expectation)

    def test_multi_column(self):
        sql = 'CREATE INDEX foo ON bar (baz, qux)'
        expectation = {
            'columns': [
                {'name': 'baz', 'null_order': None, 'order': None},
                {'name': 'qux', 'null_order': None, 'order': None}
            ],
            'condition': None,
            'name': 'foo',
            'options': {},
            'relation': {'inh': True,
                         'location': 20,
                         'relname': 'bar',
                         'relpersistence': 'p'},
            'tablespace': None,
            'type': 'btree',
            'unique': False}
        self.assertDictEqual(sql_parse.parse(sql), expectation)

    def test_multi_column_with_condition(self):
        sql = 'CREATE INDEX foo ON bar (baz, qux) WHERE baz IS NULL'
        expectation = {
            'columns': [
                {'name': 'baz', 'null_order': None, 'order': None},
                {'name': 'qux', 'null_order': None, 'order': None}
            ],
            'condition': None,
            'name': 'foo',
            'options': {},
            'relation': {'inh': True,
                         'location': 20,
                         'relname': 'bar',
                         'relpersistence': 'p'},
            'tablespace': None,
            'type': 'btree',
            'unique': False}
        self.assertDictEqual(sql_parse.parse(sql), expectation)

    def test_multi_column_with_and_condition(self):
        sql = """\
        CREATE INDEX foo ON bar (baz, qux)
               WHERE baz IS NULL AND qux IS NOT NULL"""
        expectation = {
            'columns': [
                {'name': 'baz', 'null_order': None, 'order': None},
                {'name': 'qux', 'null_order': None, 'order': None}
            ],
            'condition': None,
            'name': 'foo',
            'options': {},
            'relation': {'inh': True,
                         'location': 28,
                         'relname': 'bar',
                         'relpersistence': 'p'},
            'tablespace': None,
            'type': 'btree',
            'unique': False}
        self.assertDictEqual(sql_parse.parse(sql), expectation)

    def test_multi_column_with_or_condition(self):
        sql = """\
            CREATE INDEX foo ON bar (baz, qux)
                   WHERE baz IS NULL OR qux IS NULL"""
        expectation = {
            'columns': [
                {'name': 'baz', 'null_order': None, 'order': None},
                {'name': 'qux', 'null_order': None, 'order': None}
            ],
            'condition': None,
            'name': 'foo',
            'options': {},
            'relation': {'inh': True,
                         'location': 32,
                         'relname': 'bar',
                         'relpersistence': 'p'},
            'tablespace': None,
            'type': 'btree',
            'unique': False}
        self.assertDictEqual(sql_parse.parse(sql), expectation)

    def test_multi_column_with_sorting(self):
        sql = 'CREATE INDEX foo ON bar (baz ASC, qux DESC)'
        expectation = {
            'columns': [
                {'name': 'baz', 'null_order': None, 'order': 'ASC'},
                {'name': 'qux', 'null_order': None, 'order': 'DESC'}
            ],
            'condition': None,
            'name': 'foo',
            'options': {},
            'relation': {'inh': True,
                         'location': 20,
                         'relname': 'bar',
                         'relpersistence': 'p'},
            'tablespace': None,
            'type': 'btree',
            'unique': False}
        self.assertDictEqual(sql_parse.parse(sql), expectation)

    def test_multi_column_with_sorting_and_null_ordering(self):
        sql = '\
            CREATE INDEX foo ON bar (baz ASC NULLS LAST, qux DESC NULLS FIRST)'
        expectation = {
            'columns': [
                {'name': 'baz', 'null_order': 'LAST', 'order': 'ASC'},
                {'name': 'qux', 'null_order': 'FIRST', 'order': 'DESC'}
            ],
            'condition': None,
            'name': 'foo',
            'options': {},
            'relation': {'inh': True,
                         'location': 32,
                         'relname': 'bar',
                         'relpersistence': 'p'},
            'tablespace': None,
            'type': 'btree',
            'unique': False}
        self.assertDictEqual(sql_parse.parse(sql), expectation)

    def test_unique(self):
        sql = 'CREATE UNIQUE INDEX foo ON bar (baz)'
        expectation = {
            'columns': [{'name': 'baz', 'null_order': None, 'order': None}],
            'condition': None,
            'name': 'foo',
            'options': {},
            'relation': {'inh': True,
                         'location': 27,
                         'relname': 'bar',
                         'relpersistence': 'p'},
            'tablespace': None,
            'type': 'btree',
            'unique': True}
        self.assertDictEqual(sql_parse.parse(sql), expectation)

    def test_tablespace(self):
        sql = 'CREATE INDEX foo ON bar (baz) TABLESPACE qux'
        expectation = {
            'columns': [{'name': 'baz', 'null_order': None, 'order': None}],
            'condition': None,
            'name': 'foo',
            'options': {},
            'relation': {'inh': True,
                         'location': 20,
                         'relname': 'bar',
                         'relpersistence': 'p'},
            'tablespace': 'qux',
            'type': 'btree',
            'unique': False}
        self.assertDictEqual(sql_parse.parse(sql), expectation)

    def test_storage_parameter(self):
        sql = 'CREATE INDEX foo ON bar (baz) WITH (fillfactor = 50)'
        expectation = {
            'columns': [{'name': 'baz', 'null_order': None, 'order': None}],
            'condition': None,
            'name': 'foo',
            'options': {
                'fillfactor': 50
            },
            'relation': {'inh': True,
                         'location': 20,
                         'relname': 'bar',
                         'relpersistence': 'p'},
            'tablespace': None,
            'type': 'btree',
            'unique': False}
        self.assertDictEqual(sql_parse.parse(sql), expectation)

    def test_include(self):
        """test_include

        Unsupported Postgres 11+ feature"""
        with self.assertRaises(pglast.parser.ParseError):
            sql_parse.parse('CREATE INDEX foo ON bar (baz) INCLUDE (qux)')
