import unittest

from pglifecycle import utils


class QuoteIdentTestCase(unittest.TestCase):

    def test_variation1(self):
        self.assertEqual(utils.quote_ident('uuid-ossp'), '"uuid-ossp"')

    def test_variation2(self):
        self.assertEqual(utils.quote_ident('Foo bar'), '"Foo bar"')

    def test_variation3(self):
        self.assertEqual(utils.quote_ident('foo"bar"baz'), '"foo""bar""baz"')

    def test_variation4(self):
        self.assertEqual(utils.quote_ident('foo_bar'), 'foo_bar')


class PostgresValueTestCase(unittest.TestCase):

    def test_int(self):
        self.assertEqual(utils.postgres_value(1), '1')

    def test_str(self):
        self.assertEqual(utils.postgres_value('foo'), "'foo'")

    def test_str_with_single_quote(self):
        self.assertEqual(utils.postgres_value("foo'bar"), "$$foo'bar$$")

    def test_array(self):
        self.assertEqual(utils.postgres_value(["foo'bar", 'baz']),
                         "ARRAY[$$foo'bar$$, 'baz']")

    def test_nested_array(self):
        self.assertEqual(utils.postgres_value([[1, 2, 3], [4, 5, 6]]),
                         'ARRAY[[1, 2, 3], [4, 5, 6]]')


class SplitNameTestCase(unittest.TestCase):

    def test_variation1(self):
        self.assertEqual(utils.split_name('foo.bar'), ('foo', 'bar'))

    def test_variation2(self):
        self.assertEqual(utils.split_name('baz'), ('', 'baz'))

    def test_variation3(self):
        self.assertEqual(
            utils.split_name('qux.quux(quuz qux.grault, waldo int)'),
            ('qux', 'quux(quuz qux.grault, waldo int)'))
