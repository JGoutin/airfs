#! /usr/bin/env python
#  coding=utf-8
"""Python Cloud Object Storage I/O setup script

run "./setup.py --help-commands" for help.
"""
from datetime import datetime
from os import chdir
from os.path import dirname, abspath, join
from sys import argv

from setuptools import setup, find_packages

# Sets Package information
PACKAGE_INFO = dict(
    name='pycosio',
    description='Python Cloud Object Storage I/O',
    long_description_content_type='text/markdown; charset=UTF-8',
    classifiers=[
        # Must be listed on: https://pypi.org/classifiers/
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Topic :: System :: Filesystems',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Operating System :: OS Independent'],
    keywords='cloud cloud-storage bucket io stream',
    author='Accelize',
    author_email='info@accelize.com',
    url='https://github.com/Accelize/pycosio',
    project_urls={
        'Documentation': 'https://pycosio.readthedocs.io',
        'Download': 'https://pypi.org/project/pycosio'},
    license='Apache License, Version 2.0',
    python_requires='>=2.7,!=3.0.*,!=3.1.*,!=3.2.*,!=3.3.*',
    install_requires=[
        'requests>=2.9.0', 'python-dateutil>=2.6.0'

        # Python 2.7/3.4 compatibility
        'futures>=3.1.1; python_version == "2.7"',
        'scandir>=1.5; python_version <= "3.4"'],

    extras_require={
        # Storage specific requirements
        'oss': ['oss2>=2.3.0'],
        's3': ['boto3>=1.5.0'],
        'swift': ['python-swiftclient[keystone]>=3.3.0']},
    setup_requires=['setuptools'],
    tests_require=['pytest'],
    packages=find_packages(exclude=['docs', 'tests']),
    zip_safe=True, command_options={})

# Gets package __version__ from package
SETUP_DIR = abspath(dirname(__file__))
with open(join(SETUP_DIR, 'pycosio', '__init__.py')) as source_file:
    for line in source_file:
        if line.rstrip().startswith('__version__'):
            PACKAGE_INFO['version'] = line.split('=', 1)[1].strip(" \"\'\n")
            break

# Gets long description from readme
with open(join(SETUP_DIR, 'README.md')) as source_file:
    PACKAGE_INFO['long_description'] = source_file.read()

# Add pytest_runner requirement if needed
if {'pytest', 'test', 'ptr'}.intersection(argv):
    PACKAGE_INFO['setup_requires'].append('pytest-runner')

# Add Sphinx requirements if needed
elif 'build_sphinx' in argv:
    PACKAGE_INFO['setup_requires'] += ['sphinx', 'sphinx_rtd_theme']

# Generates wildcard "all" extras_require
PACKAGE_INFO['extras_require']['all'] = list(set(
    requirement for extra in PACKAGE_INFO['extras_require']
    for requirement in PACKAGE_INFO['extras_require'][extra]))

# Gets Sphinx configuration
PACKAGE_INFO['command_options']['build_sphinx'] = {
    'project': ('setup.py', PACKAGE_INFO['name'].capitalize()),
    'version': ('setup.py', PACKAGE_INFO['version']),
    'release': ('setup.py', PACKAGE_INFO['version']),
    'copyright': ('setup.py', '2018-%s, %s' % (
        datetime.now().year, PACKAGE_INFO['author']))}

# Runs setup
if __name__ == '__main__':
    chdir(SETUP_DIR)
    setup(**PACKAGE_INFO)
