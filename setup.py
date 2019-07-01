#!/usr/bin/env python3
import setuptools

setuptools.setup(entry_points={
    'console_scripts': ['pglifecycle=pglifecycle.cli:run']})
