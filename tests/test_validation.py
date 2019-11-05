"""Test that validation works as expected"""
import unittest

from pglifecycle import validation


class TestCase(unittest.TestCase):

    def test_invalid_object_type(self):
        with self.assertRaises(FileNotFoundError):
            validation.validate_object('foo', 'test', {})

    def test_role_happy_path(self):
        data = {
            'name': 'test',
            'comment': 'This is a test role',
            'grants': {
                'databases': {
                    'foo': ['CREATE']
                }
            },
            'options': {
                'inherit': True,
                'superuser': True
            }
        }
        self.assertTrue(validation.validate_object('ROLE', 'test', data))

    def test_invalid_role(self):
        data = {
            'name': 'test',
            'grants': {
                'database': {
                    'foo': ['CREATE']
                }
            },
            'options': {
                'inherit': 'no',
                'super_user': 100
            }
        }
        self.assertFalse(validation.validate_object('ROLE', 'test', data))
