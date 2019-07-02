import unittest

from pglifecycle import sql_parse


class TestCase(unittest.TestCase):

    def test_example1(self):
        sql = """\
        CREATE RULE "_RETURN" AS ON SELECT TO t1 DO INSTEAD SELECT * FROM t2"""
        expectation = {
            'action': 'SELECT * FROM t2',
            'event': 'SELECT',
            'instead': True,
            'name': '_RETURN',
            'table': 't1',
            'replace': False,
            'where': None
        }
        self.assertDictEqual(sql_parse.parse(sql), expectation)

    def test_example2_update(self):
        sql = """\
        CREATE RULE notify_me AS ON UPDATE TO mytable DO ALSO NOTIFY mytable"""
        expectation = {
            'action': 'NOTIFY mytable',
            'event': 'UPDATE',
            'instead': False,
            'name': 'notify_me',
            'table': 'mytable',
            'replace': False,
            'where': None
        }
        self.assertDictEqual(sql_parse.parse(sql), expectation)

    def test_example2_insert(self):
        sql = """\
        CREATE RULE notify_me AS ON INSERT TO mytable DO ALSO NOTIFY mytable"""
        expectation = {
            'action': 'NOTIFY mytable',
            'event': 'INSERT',
            'instead': False,
            'name': 'notify_me',
            'table': 'mytable',
            'replace': False,
            'where': None
        }
        self.assertDictEqual(sql_parse.parse(sql), expectation)

    def test_example2_delete(self):
        sql = """\
        CREATE RULE notify_me AS ON DELETE TO mytable DO ALSO NOTIFY mytable"""
        expectation = {
            'action': 'NOTIFY mytable',
            'event': 'DELETE',
            'instead': False,
            'name': 'notify_me',
            'table': 'mytable',
            'replace': False,
            'where': None
        }
        self.assertDictEqual(sql_parse.parse(sql), expectation)
