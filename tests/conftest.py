"""Pytest configuration."""

from os import environ as _environ

_environ["AIRFS_FULLTRACEBACK"] = "1"


def pytest_addoption(parser):
    """Add command lines arguments.

    Args:
        parser: Parser.
    """
    parser.addoption(
        "--mount",
        action="store",
        default="./test_mount.json",
        help="Specify a file of defining storage to mount and tests.",
    )
    parser.addoption(
        "--filter_id",
        action="store",
        help=(
            'Allow to run only a subset of tests specified with "--mount". '
            "Comma separated list of tests ID to run (ID: <storage>_<id_prefix>)."
        ),
    )


def pytest_generate_tests(metafunc):
    """Generate test for mounted storage.

    Args:
        metafunc: Metafunc.
    """
    if "storage_test_kwargs" in metafunc.fixturenames:
        config_path = metafunc.config.getoption("mount")
        from os.path import isfile

        if isfile(config_path):
            from json import load

            with open(config_path) as config_file:
                configs = load(config_file)
        else:
            configs = dict()

        filter_id = metafunc.config.getoption("filter_id")
        if filter_id:
            filter_id = filter_id.split(",")

        from airfs import mount

        argvalues = []
        ids = []
        for storage_kwargs in configs:
            tester_kwargs = dict()
            for key in tuple(storage_kwargs):
                if key.startswith("test."):
                    tester_kwargs[key.split(".", 1)[1]] = storage_kwargs.pop(key)

            test_id = [storage_kwargs["storage"]]
            try:
                test_id.append(tester_kwargs.pop("id_suffix"))
            except KeyError:
                pass
            test_id = "_".join(test_id)

            if filter_id and test_id not in filter_id:
                continue

            tester_kwargs["storage_info"] = tuple(mount(**storage_kwargs).items())[0][1]

            ids.append(test_id)
            argvalues.append(tester_kwargs)

        metafunc.parametrize("storage_test_kwargs", argvalues, ids=ids)
