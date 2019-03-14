# coding=utf-8
"""Pytest configuration"""


def pytest_addoption(parser):
    """
    Add command lines arguments
    """
    parser.addoption(
        "--mount", action="store", default="./mount.json",
        help='Specify a file of defining storage to mount and tests.')


def pytest_generate_tests(metafunc):
    """
    Generate test for mounted storage.
    """
    if 'storage' in metafunc.fixturenames:
        config_path = metafunc.config.getoption('mount')
        from os.path import isfile
        if isfile(config_path):
            from json import load
            with open(config_path) as config_file:
                config = load(config_file)
        else:
            config = dict()

        from pycosio import mount
        argvalues = []
        ids = []
        for storage_kwargs in config:
            name, system_info = tuple(mount(**storage_kwargs).items())[0]
            argvalues.append(system_info)
            ids.append(name)

        metafunc.parametrize("storage", argvalues, ids=ids)
