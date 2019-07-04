import unittest

from pglifecycle import sql_parse


class TestCase(unittest.TestCase):

    def test_example1(self):
        sql = """\
            DELETE FROM films
                  USING producers
                  WHERE producer_id = producers.id
                    AND producers.name = 'foo';"""
        expectation = ('DELETE FROM films USING producers WHERE producer_id '
                       "= producers.id AND producers.name = 'foo'")
        self.assertEqual(sql_parse.parse(sql), expectation)

    def test_example2(self):
        sql = """\
            DELETE FROM films
                  WHERE producer_id IN
                        (SELECT id
                           FROM producers
                          WHERE name = 'foo');"""
        expectation = ('DELETE FROM films WHERE producer_id IN '
                       "(SELECT id FROM producers WHERE name = 'foo')")
        self.assertEqual(sql_parse.parse(sql), expectation)

    def test_example3(self):
        sql = "DELETE FROM films WHERE kind <> 'Musical';"
        self.assertEqual(sql_parse.parse(sql), sql[:-1])

    def test_example4(self):
        sql = 'DELETE FROM films;'
        self.assertEqual(sql_parse.parse(sql), sql[:-1])

    def test_example5(self):
        sql = "DELETE FROM tasks WHERE status = 'DONE' RETURNING *;"
        self.assertEqual(sql_parse.parse(sql), sql[:-1])

    def test_example6(self):
        sql = 'DELETE FROM tasks WHERE CURRENT OF c_tasks;'
        self.assertEqual(sql_parse.parse(sql), sql[:-1])
