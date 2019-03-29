[![Linux Build Status](https://travis-ci.org/Accelize/pycosio.svg?branch=master)](https://travis-ci.org/Accelize/pycosio)
[![Windows Build status](https://ci.appveyor.com/api/projects/status/g4n3jdk2a5sx0cp3?svg=true)](https://ci.appveyor.com/project/accelize-application/pycosio)
[![codecov](https://codecov.io/gh/Accelize/pycosio/branch/master/graph/badge.svg)](https://codecov.io/gh/Accelize/pycosio)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/0c9fc64f5fe94defac90140d769e1de3)](https://www.codacy.com/app/Accelize/pycosio?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=Accelize/pycosio&amp;utm_campaign=Badge_Grade)
[![Documentation Status](https://readthedocs.org/projects/pycosio/badge/?version=latest)](https://pycosio.readthedocs.io/en/latest/?badge=latest)
[![PyPI](https://img.shields.io/pypi/v/pycosio.svg)](https://pypi.org/project/pycosio)

Pycosio (Python Cloud Object Storage I/O)
=========================================

For more information, read the [Pycosio documentation](https://pycosio.readthedocs.io).

Pycosio brings standard Python I/O to cloud objects by providing:

* Abstract classes of Cloud objects with the complete ``io.RawIOBase`` and
  ``io.BufferedIOBase`` standard interfaces.
* Features equivalent to the standard library (``io``, ``os``, ``os.path``,
  ``shutil``) for seamlessly managing cloud objects and local files.

These functions are source agnostic and always provide the same interface for
all files from cloud storage or local file systems.

Buffered cloud objects also support the following features:

* Buffered asynchronous writing of any object size.
* Buffered asynchronous preloading in reading mode.
* Write or read lock depending on memory usage limitation.
* Maximization of bandwidth using parallels connections.

Supported Cloud storage
-----------------------

Pycosio is compatible with the following cloud objects storage services:

* Alibaba Cloud OSS
* Amazon Web Services S3
* Microsoft Azure Blobs Storage
* Microsoft Azure Files Storage
* OpenStack Swift

Pycosio can also access any publicly accessible file via HTTP/HTTPS
(Read only).
