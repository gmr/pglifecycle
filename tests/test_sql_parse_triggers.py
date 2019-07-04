import unittest

from pglifecycle import sql_parse


class TestCase(unittest.TestCase):

    def test_simple(self):
        sql = """\
            CREATE TRIGGER check_update
                BEFORE UPDATE ON accounts
                FOR EACH ROW
                EXECUTE PROCEDURE check_account_update();"""
        expectation = {
            'when': 'BEFORE',
            'events': ['UPDATE'],
            'name': 'check_update',
            'relation': 'accounts',
            'transitions': [],
            'row': True,
            'condition': None,
            'function': 'check_account_update()'
        }
        self.assertDictEqual(sql_parse.parse(sql), expectation)

    def test_multiple_events(self):
        sql = """\
            CREATE TRIGGER replicate
                AFTER INSERT OR UPDATE OR DELETE ON accounts
                FOR EACH ROW
                EXECUTE PROCEDURE replicate_data();"""
        expectation = {
            'when': 'AFTER',
            'events': ['INSERT', 'UPDATE', 'DELETE'],
            'name': 'replicate',
            'relation': 'accounts',
            'transitions': [],
            'row': True,
            'condition': None,
            'function': 'replicate_data()'
        }
        self.assertDictEqual(sql_parse.parse(sql), expectation)

    def test_with_condition(self):
        sql = """\
            CREATE TRIGGER log_update
                AFTER UPDATE ON accounts
                FOR EACH ROW
                WHEN (OLD.* IS DISTINCT FROM NEW.*)
                EXECUTE PROCEDURE log_account_update();"""
        expectation = {
            'when': 'AFTER',
            'events': ['UPDATE'],
            'relation': 'accounts',
            'name': 'log_update',
            'transitions': [],
            'row': True,
            'condition': 'OLD.* IS DISTINCT FROM NEW.*',
            'function': 'log_account_update()'
        }
        self.assertDictEqual(sql_parse.parse(sql), expectation)

    def test_instead(self):
        sql = """\
            CREATE TRIGGER view_insert
                INSTEAD OF INSERT ON my_view
                FOR EACH ROW
                EXECUTE PROCEDURE view_insert_row();"""
        expectation = {
            'when': 'INSTEAD OF',
            'events': ['INSERT'],
            'relation': 'my_view',
            'name': 'view_insert',
            'transitions': [],
            'row': True,
            'condition': None,
            'function': 'view_insert_row()'
        }
        self.assertDictEqual(sql_parse.parse(sql), expectation)

    def test_referencing(self):
        sql = """\
            CREATE TRIGGER transfer_insert
                AFTER INSERT ON transfer
                REFERENCING NEW TABLE AS inserted
                FOR EACH STATEMENT
                EXECUTE PROCEDURE check_transfer_balances_to_zero();"""
        expectation = {
            'when': 'AFTER',
            'events': ['INSERT'],
            'relation': 'transfer',
            'name': 'transfer_insert',
            'transitions': [{
                'name': 'inserted',
                'is_new': True,
                'is_table': True
            }],
            'row': False,
            'condition': None,
            'function': 'check_transfer_balances_to_zero()'
        }
        self.assertDictEqual(sql_parse.parse(sql), expectation)

    def test_referencing_two_tables(self):
        sql = """\
            CREATE TRIGGER paired_items_update
                AFTER UPDATE ON paired_items
                REFERENCING NEW TABLE AS newtab OLD TABLE AS oldtab
                FOR EACH ROW
                EXECUTE PROCEDURE check_matching_pairs();"""
        expectation = {
            'when': 'AFTER',
            'events': ['UPDATE'],
            'relation': 'paired_items',
            'name': 'paired_items_update',
            'transitions': [
                {
                    'name': 'newtab',
                    'is_new': True,
                    'is_table': True
                },
                {
                    'name': 'oldtab',
                    'is_new': False,
                    'is_table': True
                }
            ],
            'row': True,
            'condition': None,
            'function': 'check_matching_pairs()'
        }
        self.assertDictEqual(sql_parse.parse(sql), expectation)
