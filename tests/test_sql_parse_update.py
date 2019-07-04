import unittest

from pglifecycle import sql_parse


class TestCase(unittest.TestCase):

    def test_example1(self):
        sql = "UPDATE films SET kind = 'Dramatic' WHERE kind = 'Drama';"
        self.assertEqual(sql_parse.parse(sql), sql[:-1])

    def test_example2(self):
        sql = """\
            UPDATE weather
               SET temp_lo = temp_lo+1, temp_hi = temp_lo+15, prcp = DEFAULT
             WHERE city = 'San Francisco'
               AND date = '2003-07-03';"""
        expectation = ('UPDATE weather SET temp_lo = temp_lo + 1, temp_hi = '
                       'temp_lo + 15, prcp = DEFAULT WHERE city = '
                       "'San Francisco' AND date = '2003-07-03'")
        self.assertEqual(sql_parse.parse(sql), expectation)

    def test_example3(self):
        sql = """\
            UPDATE weather
               SET temp_lo = temp_lo+1, temp_hi = temp_lo+15, prcp = DEFAULT
             WHERE city = 'San Francisco'
               AND date = '2003-07-03'
         RETURNING temp_lo, temp_hi, prcp;"""
        expectation = ('UPDATE weather SET temp_lo = temp_lo + 1, temp_hi = '
                       'temp_lo + 15, prcp = DEFAULT WHERE city = '
                       "'San Francisco' AND date = '2003-07-03' RETURNING "
                       'temp_lo, temp_hi, prcp')
        self.assertEqual(sql_parse.parse(sql), expectation)

    def test_example4(self):
        sql = """\
            UPDATE weather
               SET (temp_lo, temp_hi, prcp) =
                   (temp_lo + 1, temp_lo + 15, DEFAULT)
             WHERE city = 'San Francisco'
               AND date = '2003-07-03';"""
        expectation = ('UPDATE weather SET (temp_lo, temp_hi, prcp) = '
                       '(temp_lo + 1, temp_lo + 15, DEFAULT) WHERE city = '
                       "'San Francisco' AND date = '2003-07-03'")
        self.assertEqual(sql_parse.parse(sql), expectation)

    def test_example5(self):
        sql = """\
            UPDATE employees
               SET sales_count = sales_count + 1
              FROM accounts
             WHERE accounts.name = 'Acme Corporation'
               AND employees.id = accounts.sales_person;"""
        expectation = ('UPDATE employees SET sales_count = sales_count + 1 '
                       'FROM accounts WHERE accounts.name = '
                       "'Acme Corporation' AND employees.id = "
                       'accounts.sales_person')
        self.assertEqual(sql_parse.parse(sql), expectation)

    def test_example6(self):
        sql = """\
            UPDATE employees
               SET sales_count = sales_count + 1
             WHERE id = (SELECT sales_person
                           FROM accounts
                          WHERE name = 'Acme Corporation');"""
        expectation = ('UPDATE employees SET sales_count = sales_count + 1 '
                       'WHERE id = (SELECT sales_person FROM accounts WHERE '
                       "name = 'Acme Corporation')")
        self.assertEqual(sql_parse.parse(sql), expectation)

    def test_example7(self):
        sql = """\
            UPDATE accounts
               SET (contact_first_name, contact_last_name) =
                   (SELECT first_name, last_name
                      FROM salesmen
                     WHERE salesmen.id = accounts.sales_id);"""
        expectation = ('UPDATE accounts SET (contact_first_name, '
                       'contact_last_name) = (SELECT first_name, last_name '
                       'FROM salesmen WHERE salesmen.id = accounts.sales_id)')
        self.assertEqual(sql_parse.parse(sql), expectation)

    def test_example8(self):
        sql = """\
            UPDATE accounts
               SET contact_first_name = first_name,
                   contact_last_name = last_name
              FROM salesmen
             WHERE salesmen.id = accounts.sales_id;"""
        expectation = ('UPDATE accounts SET contact_first_name = first_name, '
                       'contact_last_name = last_name FROM salesmen WHERE '
                       'salesmen.id = accounts.sales_id')
        self.assertEqual(sql_parse.parse(sql), expectation)

    def test_example9(self):
        sql = """\
            UPDATE summary s
               SET (sum_x, sum_y, avg_x, avg_y) =
                   (SELECT sum(x), sum(y), avg(x), avg(y)
                      FROM data d
                     WHERE d.group_id = s.group_id);"""
        expectation = ('UPDATE summary AS s SET (sum_x, sum_y, avg_x, avg_y) '
                       '= (SELECT sum(x), sum(y), avg(x), avg(y) FROM data '
                       'AS d WHERE d.group_id = s.group_id)')
        self.assertEqual(sql_parse.parse(sql), expectation)

    def test_example10(self):
        sql = "UPDATE films SET kind = 'Dramatic' WHERE CURRENT OF c_films;"
        self.assertEqual(sql_parse.parse(sql), sql[:-1])
