import unittest

from pglifecycle import sql_parse


class TestCase(unittest.TestCase):

    def test_example1(self):
        sql = 'CREATE TYPE compfoo AS (f1 int, f2 text)'
        expectation = {
            'name': 'compfoo',
            'attributes': [{'name': 'f1', 'type': 'int4'},
                           {'name': 'f2', 'type': 'text'}]}
        self.assertDictEqual(sql_parse.parse(sql), expectation)

    def test_example2(self):
        sql = "CREATE TYPE bug_status AS ENUM ('new', 'open', 'closed');"
        expectation = {
            'name': 'bug_status',
            'values': ['new', 'open', 'closed']}
        self.assertDictEqual(sql_parse.parse(sql), expectation)

    def test_example3(self):
        sql = """\
        CREATE TYPE float8_range AS
              RANGE (subtype = float8, subtype_diff = float8mi)"""
        expectation = {
            'name': 'float8_range',
            'range': {'subtype': 'float8', 'subtype_diff': 'float8mi'}}
        self.assertDictEqual(sql_parse.parse(sql), expectation)

    def test_example4(self):
        sql = """\
        CREATE TYPE box (
            INTERNALLENGTH = 16,
            INPUT = my_box_in_function,
            OUTPUT = my_box_out_function)"""
        expectation = {
            'name': 'box',
            'definition': {
                'input': 'my_box_in_function',
                'internallength': 16,
                'output': 'my_box_out_function'}}
        self.assertDictEqual(sql_parse.parse(sql), expectation)

    def test_example5(self):
        sql = """\
        CREATE TYPE complex (
           internallength = 16,
           input = complex_in,
           output = complex_out,
           receive = complex_recv,
           send = complex_send,
           alignment = double
        );"""
        expectation = {
            'name': 'complex',
            'definition': {
                'input': 'complex_in',
                'internallength': 16,
                'output': 'complex_out',
                'receive': 'complex_recv',
                'send': 'complex_send',
                'alignment': 'double'}}
        self.assertDictEqual(sql_parse.parse(sql), expectation)
