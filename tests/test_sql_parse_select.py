import unittest

from pglifecycle import sql_parse


class TestCase(unittest.TestCase):

    def test_example1(self):
        sql = 'SELECT * FROM name'
        self.assertEqual(sql_parse.parse(sql), sql)

    def test_example2(self):
        sql = """\
        SELECT f.title, f.did, d.name, f.date_prod, f.kind
          FROM distributors d, films f
         WHERE f.did = d.did;"""
        expectation = ('SELECT f.title, f.did, d.name, f.date_prod, f.kind '
                       'FROM distributors AS d, films AS f '
                       'WHERE f.did = d.did')
        self.assertEqual(sql_parse.parse(sql), expectation)

    def test_example3(self):
        sql = 'SELECT kind, sum(len) AS total FROM films GROUP BY kind;'
        expectation = 'SELECT kind, sum(len) AS total FROM films GROUP BY kind'
        self.assertEqual(sql_parse.parse(sql), expectation)

    def test_example4(self):
        sql = """\
            SELECT kind, sum(len) AS total
              FROM films
          GROUP BY kind HAVING sum(len) < interval '5 hours';"""
        expectation = ('SELECT kind, sum(len) AS total FROM films '
                       "GROUP BY kind HAVING sum(len) < '5 hours'::interval")
        self.assertEqual(sql_parse.parse(sql), expectation)

    def test_example5(self):
        sql = 'SELECT * FROM distributors ORDER BY name;'
        expectation = 'SELECT * FROM distributors ORDER BY name'
        self.assertEqual(sql_parse.parse(sql), expectation)

    def test_example5b(self):
        sql = 'SELECT * FROM distributors ORDER BY 2;'
        expectation = 'SELECT * FROM distributors ORDER BY 2'
        self.assertEqual(sql_parse.parse(sql), expectation)

    def test_example6(self):
        sql = """\
            SELECT distributors.name
              FROM distributors
             WHERE distributors.name LIKE 'W%'
             UNION
            SELECT actors.name
              FROM actors
             WHERE actors.name LIKE 'W%';"""
        expectation = ('SELECT distributors.name FROM distributors '
                       "WHERE distributors.name LIKE 'W%' UNION "
                       'SELECT actors.name FROM actors '
                       "WHERE actors.name LIKE 'W%'")
        self.assertEqual(sql_parse.parse(sql), expectation)

    def test_example7(self):
        sql = 'SELECT * FROM distributors_2(111) AS (f1 int, f2 text);'
        expectation = 'SELECT * FROM distributors_2(111) AS (f1 int4, f2 text)'
        self.assertEqual(sql_parse.parse(sql), expectation)

    def test_example8(self):
        sql = """\
            SELECT *
              FROM unnest(ARRAY['a', 'b', 'c', 'd', 'e', 'f'])
                   WITH ORDINALITY"""
        expectation = ("SELECT * FROM unnest(ARRAY['a', 'b', 'c', 'd', "
                       "'e', 'f']) WITH ORDINALITY")
        self.assertEqual(sql_parse.parse(sql), expectation)

    def test_example9(self):
        sql = """\
              WITH t AS (
                  SELECT random() AS x FROM generate_series(1, 3))
                  SELECT *
                    FROM t UNION ALL
                  SELECT *
                    FROM t"""
        expectation = ('WITH t AS (SELECT random() AS x FROM '
                       'generate_series(1, 3)) SELECT * FROM t UNION ALL '
                       'SELECT * FROM t')
        self.assertEqual(sql_parse.parse(sql), expectation)

    def test_example10(self):
        sql = """\
              WITH RECURSIVE employee_recursive(distance,
                                                employee_name,
                                                manager_name) AS
           (SELECT 1, employee_name, manager_name
              FROM employee
             WHERE manager_name = 'Mary'
                   UNION ALL
            SELECT er.distance + 1, e.employee_name, e.manager_name
              FROM employee_recursive er, employee e
             WHERE er.employee_name = e.manager_name)
            SELECT distance, employee_name
              FROM employee_recursive;"""  # noqa: Q448
        expectation = ('WITH RECURSIVE employee_recursive(distance, '
                       'employee_name, manager_name) AS (SELECT 1, '
                       'employee_name, manager_name FROM employee WHERE '
                       "manager_name = 'Mary' UNION ALL SELECT "
                       'er.distance + 1, e.employee_name, e.manager_name '
                       'FROM employee_recursive AS er, employee AS e '
                       'WHERE er.employee_name = e.manager_name) '
                       'SELECT distance, employee_name '
                       'FROM employee_recursive')
        self.assertEqual(sql_parse.parse(sql), expectation)

    def test_example11(self):
        sql = """\
            SELECT m.name AS mname, pname
              FROM manufacturers m,
                   LATERAL get_product_names(m.id) pname;"""
        expectation = ('SELECT m.name AS mname, pname FROM manufacturers '
                       'AS m, LATERAL get_product_names(m.id) AS pname')
        self.assertEqual(sql_parse.parse(sql), expectation)

    def test_example12(self):
        sql = """\
            SELECT m.name AS mname, pname
              FROM manufacturers m
         LEFT JOIN LATERAL get_product_names(m.id) pname ON true;"""
        expectation = ('SELECT m.name AS mname, pname FROM manufacturers AS m '
                       'LEFT JOIN LATERAL get_product_names(m.id) AS pname '
                       'ON TRUE')
        self.assertEqual(sql_parse.parse(sql), expectation)
