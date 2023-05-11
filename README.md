![Tests](https://github.com/JGoutin/airfs/workflows/tests/badge.svg)
[![codecov](https://codecov.io/gh/JGoutin/airfs/branch/master/graph/badge.svg)](https://codecov.io/gh/JGoutin/airfs)
[![PyPI](https://img.shields.io/pypi/v/airfs.svg)](https://pypi.org/project/airfs)

airfs: A Python library for cloud and remote file Systems
=========================================================

airfs brings standard Python I/O to various storages (like cloud objects storage, remote
file-systems, ...) by providing:

* Abstract classes of Cloud objects with the complete ``io.RawIOBase`` and
  ``io.BufferedIOBase`` standard interfaces.
* Features equivalent to the standard library (``io``, ``os``, ``os.path``, ``shutil``)
  for seamlessly managing storage objects and local files.

These functions are source agnostic and always provide the same interface for all files
from storage or local file systems.

Buffered storage objects also support the following features:

* Buffered asynchronous writing of any object size.
* Buffered asynchronous preloading in reading mode.
* Write or read lock depending on memory usage limitation.
* Maximization of bandwidth using parallels connections.

For more information, refer to the [documentation](https://jgoutin.github.io/airfs/).

Supported storage
-----------------

airfs is compatible with the following storage services:

* Alibaba Cloud OSS
* AWS S3 / [MinIO](https://github.com/minio/minio)
* GitHub (Read Only)
* Microsoft Azure Blobs Storage
* Microsoft Azure Files Storage
* OpenStack Swift Object Store

airfs can also access any publicly accessible file via HTTP/HTTPS (Read only).

---

*"airfs" is a fork of the unmaintained "[Pycosio](https://github.com/Accelize/pycosio)" 
project by its main developer.*
