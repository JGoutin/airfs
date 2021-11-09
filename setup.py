#! /usr/bin/env python3
"""Setup script

run "./setup.py --help-commands" for help.
"""
from datetime import datetime
from os import chdir
from os.path import dirname, abspath, join
from sys import argv

from setuptools import setup, find_packages  # type: ignore

PACKAGE_INFO = dict(
    name="airfs",
    description="A Python library for cloud and remote file Systems",
    long_description_content_type="text/markdown; charset=UTF-8",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Topic :: System :: Filesystems",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Operating System :: OS Independent",
    ],
    keywords=(
        "file-system storage io stream cloud cloud-storage blob bucket alibaba aws "
        "azure github http openstack oss s3 swift"
    ),
    author="J.Goutin",
    url="https://github.com/JGoutin/airfs",
    project_urls={
        "Documentation": "https://airfs.readthedocs.io",
        "Download": "https://pypi.org/project/airfs",
    },
    license="Apache License, Version 2.0",
    python_requires=">=3.6",
    install_requires=[
        "requests>=2.20.0",
        "python-dateutil>=2.6.0"
        # Python compatibility back ports
        'importlib_resources>=2.0.0; python_version < "3.7"',
    ],
    extras_require={
        "azure_blob": ["azure-storage-blob>=1.3.0,<=2.1.0"],
        "azure_file": ["azure-storage-file>=1.3.0,<=2.1.0"],
        "oss": ["oss2>=2.3.0"],
        "s3": ["boto3>=1.5.0"],
        "swift": ["python-swiftclient[keystone]>=3.3.0"],
    },
    setup_requires=["setuptools"],
    tests_require=["pytest-cov", "pytest-mypy", "pytest-flake8", "pytest-black"],
    packages=find_packages(exclude=["docs", "tests", "tests_storage_package"]),
    zip_safe=True,
    command_options={},
)

SETUP_DIR = abspath(dirname(__file__))
with open(join(SETUP_DIR, "airfs", "__init__.py")) as source_file:
    for line in source_file:
        if line.rstrip().startswith("__version__"):
            PACKAGE_INFO["version"] = line.split("=", 1)[1].strip(" \"'\n")
            break

with open(join(SETUP_DIR, "README.md")) as source_file:
    PACKAGE_INFO["long_description"] = source_file.read()

if {"pytest", "test", "ptr"}.intersection(argv):
    PACKAGE_INFO["setup_requires"].append("pytest-runner")

elif "build_sphinx" in argv:
    PACKAGE_INFO["setup_requires"] += ["sphinx", "sphinx_rtd_theme"]

PACKAGE_INFO["extras_require"]["all"] = list(
    set(
        requirement
        for extra in PACKAGE_INFO["extras_require"]
        for requirement in PACKAGE_INFO["extras_require"][extra]
    )
)

PACKAGE_INFO["command_options"]["build_sphinx"] = {
    "project": ("setup.py", PACKAGE_INFO["name"].capitalize()),
    "version": ("setup.py", PACKAGE_INFO["version"]),
    "release": ("setup.py", PACKAGE_INFO["version"]),
    "copyright": (
        "setup.py",
        f"2018-2019, Accelize; 2020-{datetime.now().year}, {PACKAGE_INFO['author']}",
    ),
}

if __name__ == "__main__":
    chdir(SETUP_DIR)
    setup(**PACKAGE_INFO)
