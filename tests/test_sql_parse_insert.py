import unittest

from pglifecycle import sql_parse


class TestCase(unittest.TestCase):

    def test_example1(self):
        sql = """\
            INSERT INTO films
                 VALUES ('UA502', 'Bananas', 105, '1971-07-13',
                         'Comedy', '82 minutes');"""
        expectation = ("INSERT INTO films VALUES ('UA502', 'Bananas', 105, "
                       "'1971-07-13', 'Comedy', '82 minutes')")
        self.assertEqual(sql_parse.parse(sql), expectation)

    def test_example2(self):
        sql = """\
            INSERT INTO films (code, title, did, date_prod, kind)
                 VALUES ('T_601', 'Yojimbo', 106, '1961-06-16', 'Drama');"""
        expectation = ('INSERT INTO films (code, title, did, date_prod, kind) '
                       "VALUES ('T_601', 'Yojimbo', 106, '1961-06-16', 'Drama'"
                       ')')
        self.assertEqual(sql_parse.parse(sql), expectation)

    def test_example3(self):
        sql = """\
            INSERT INTO films
                 VALUES ('UA502', 'Bananas', 105, DEFAULT, 'Comedy',
                         '82 minutes');"""
        expectation = ("INSERT INTO films VALUES ('UA502', 'Bananas', 105, "
                       "DEFAULT, 'Comedy', '82 minutes')")
        self.assertEqual(sql_parse.parse(sql), expectation)

    def test_example4(self):
        sql = 'INSERT INTO films DEFAULT VALUES;'
        self.assertEqual(sql_parse.parse(sql), sql[:-1])

    def test_example5(self):
        sql = """\
            INSERT INTO films (code, title, did, date_prod, kind)
                 VALUES ('B6717', 'Tampopo', 110, '1985-02-10', 'Comedy'),
                        ('HG120', 'The Dinner', 140, DEFAULT, 'Comedy');"""
        expectation = ('INSERT INTO films (code, title, did, date_prod, kind) '
                       "VALUES ('B6717', 'Tampopo', 110, '1985-02-10', 'Comedy"
                       "'), ('HG120', 'The Dinner', 140, DEFAULT, 'Comedy')")
        self.assertEqual(sql_parse.parse(sql), expectation)

    def test_example6(self):
        sql = """\
            INSERT INTO films
                 SELECT *
                   FROM tmp_films
                  WHERE date_prod < '2004-05-07';"""
        expectation = ('INSERT INTO films SELECT * FROM tmp_films WHERE '
                       "date_prod < '2004-05-07'")
        self.assertEqual(sql_parse.parse(sql), expectation)

    def test_example7(self):
        sql = """\
            INSERT INTO tictactoe (game, board[1:3][1:3])
                 VALUES (1, '{{" "," "," "},{" "," "," "},{" "," "," "}}');"""
        expectation = ('INSERT INTO tictactoe (game, board[1:3][1:3]) VALUES '
                       "(1, '{{\" \",\" \",\" \"},{\" \",\" \",\" \"},"
                       "{\" \",\" \",\" \"}}')")
        self.assertEqual(sql_parse.parse(sql), expectation)

    def test_example8(self):
        sql = """\
            INSERT INTO distributors (did, dname)
                 VALUES (DEFAULT, 'XYZ Widgets')
              RETURNING did;"""
        expectation = ('INSERT INTO distributors (did, dname) VALUES (DEFAULT,'
                       " 'XYZ Widgets') RETURNING did")
        self.assertEqual(sql_parse.parse(sql), expectation)

    def test_example9(self):
        sql = """\
            WITH upd AS (
                 UPDATE employees
                    SET sales_count = sales_count + 1
                  WHERE id = (SELECT sales_person
                                FROM accounts
                               WHERE name = 'Acme Corporation')
              RETURNING *)
            INSERT INTO employees_log
                 SELECT *, current_timestamp
                   FROM upd;"""  # noqa: Q448
        expectation = ('WITH upd AS (UPDATE employees SET sales_count = '
                       'sales_count + 1 WHERE id = (SELECT sales_person '
                       "FROM accounts WHERE name = 'Acme Corporation') "
                       'RETURNING *) INSERT INTO employees_log SELECT *, '
                       'CURRENT_TIMESTAMP FROM upd')
        self.assertEqual(sql_parse.parse(sql), expectation)

    def test_example10(self):
        sql = """\
            INSERT INTO distributors (did, dname)
                 VALUES (5, 'Gizmo Transglobal'),
                        (6, 'Associated Computing, Inc')
                        ON CONFLICT (did) DO
                 UPDATE
                    SET dname = EXCLUDED.dname;"""
        expectation = ('INSERT INTO distributors (did, dname) VALUES (5, '
                       "'Gizmo Transglobal'), (6, 'Associated Computing, Inc')"
                       ' ON CONFLICT (did) DO UPDATE SET dname = '
                       'EXCLUDED.dname')
        self.assertEqual(sql_parse.parse(sql), expectation)

    def test_example11(self):
        sql = """\
            INSERT INTO distributors (did, dname)
                 VALUES (7, 'Redline GmbH')
                        ON CONFLICT (did) DO NOTHING;"""
        expectation = ('INSERT INTO distributors (did, dname) VALUES '
                       "(7, 'Redline GmbH') ON CONFLICT (did) DO NOTHING")
        self.assertEqual(sql_parse.parse(sql), expectation)

    def test_example12(self):
        sql = """\
            INSERT INTO distributors AS d (did, dname)
                 VALUES (8, 'Anvil Distribution')
                        ON CONFLICT (did) DO
                 UPDATE
                    SET dname = EXCLUDED.dname ||
                                ' (formerly ' || d.dname || ')'
                  WHERE d.zipcode <> '21201';"""
        expectation = ('INSERT INTO distributors AS d (did, dname) VALUES '
                       "(8, 'Anvil Distribution') ON CONFLICT (did) DO UPDATE "
                       "SET dname = EXCLUDED.dname || ' (formerly ' || d.dname"
                       " || ')' WHERE d.zipcode <> '21201'")
        self.assertEqual(sql_parse.parse(sql), expectation)

    def test_example13(self):
        sql = """\
            INSERT INTO distributors (did, dname)
                 VALUES (9, 'Antwerp Design')
                        ON CONFLICT
                        ON CONSTRAINT distributors_pkey DO NOTHING;"""
        expectation = ('INSERT INTO distributors (did, dname) VALUES '
                       "(9, 'Antwerp Design') ON CONFLICT ON CONSTRAINT "
                       'distributors_pkey DO NOTHING')
        self.assertEqual(sql_parse.parse(sql), expectation)

    def test_example14(self):
        sql = """\
            INSERT INTO distributors (did, dname)
                 VALUES (10, 'Conrad International')
                        ON CONFLICT (did)
                  WHERE is_active DO NOTHING;"""
        expectation = ('INSERT INTO distributors (did, dname) VALUES '
                       "(10, 'Conrad International') "
                       'ON CONFLICT (did) WHERE is_active DO NOTHING')
        self.assertEqual(sql_parse.parse(sql), expectation)
