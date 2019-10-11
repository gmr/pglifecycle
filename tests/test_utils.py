import unittest

from pglifecycle import utils


class QuoteIdentTestCase(unittest.TestCase):

    def test_known_value(self):
        self.assertEqual(utils.quote_ident('uuid-ossp'), '"uuid-ossp"')

    def test_double_quotes(self):
        self.assertEqual(utils.quote_ident('foo"bar"baz'), '"foo""bar""baz"')
