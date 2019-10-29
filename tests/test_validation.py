"""Test that validation works as expected"""
import unittest

from jsonschema import exceptions

from pglifecycle import validation


class TestCase(unittest.TestCase):

    def test_invalid_object_type(self):
        with self.assertRaises(FileNotFoundError):
            validation.validate_object('foo', {})

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
        self.assertTrue(validation.validate_object('ROLE', data))

    def test_invalid_role(self):
        data = {
            'name': 'test',
            'grants': {
                'database': {
                    'foo': ['CREATE']
                }
            },
            'options': {
                'inherit': True,
                'superuser': True
            }
        }
        with self.assertRaises(exceptions.ValidationError):
            validation.validate_object('ROLE', data)
